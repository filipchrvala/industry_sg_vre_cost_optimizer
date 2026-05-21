from __future__ import annotations

import importlib
from pathlib import Path
import sys
import traceback

import pandas as pd
import yaml
from domino.base_piece import BasePiece

from .models import InputModel, OutputModel

# Domino loads this module before piece_function — keep imports minimal and local.
_SIMULATE_MODULE_CANDIDATES = (
    "pieces.SimulateMRKScenarioPiece.piece",
    "pieces.SimulatePiece.piece",
)


def _load_simulate_module():
    """Same pattern as 0.1.8, with fallback for older Docker images."""
    repo_root = Path(__file__).resolve().parents[2]
    repo_s = str(repo_root)
    if repo_s not in sys.path:
        sys.path.insert(0, repo_s)
    last_err: ModuleNotFoundError | None = None
    for module_name in _SIMULATE_MODULE_CANDIDATES:
        try:
            return importlib.import_module(module_name)
        except ModuleNotFoundError as exc:
            last_err = exc
    raise ModuleNotFoundError(
        "Missing simulate module. Tried: " + ", ".join(_SIMULATE_MODULE_CANDIDATES)
    ) from last_err


def _align_load_and_solar(load_df: pd.DataFrame, solar_df: pd.DataFrame) -> pd.DataFrame:
    if "datetime" not in solar_df.columns:
        raise ValueError("virtual_solar_csv must contain datetime column")
    if "pv_kw" not in solar_df.columns:
        raise ValueError("virtual_solar_csv must contain pv_kw column")

    ld = load_df.copy()
    sd = solar_df.copy()
    ld["datetime"] = pd.to_datetime(ld["datetime"])
    sd["datetime"] = pd.to_datetime(sd["datetime"])
    merged = ld[["datetime", "load_kw"]].merge(sd[["datetime", "pv_kw"]], on="datetime", how="inner")
    if len(merged) == 0:
        raise ValueError("No overlapping datetimes between load_csv and virtual_solar_csv")
    return merged


class BatterySimulationPiece(BasePiece):
    """Generate battery SOC profile using dispatch model."""

    def piece_function(self, input_data: InputModel) -> OutputModel:
        print("[BatterySimulationPiece] piece_function START", flush=True)

        csv_path = Path(input_data.load_csv)
        scenario_path = Path(input_data.scenario_yaml)
        solar_path = Path(input_data.virtual_solar_csv)
        out_dir = Path(self.results_path or scenario_path.parent)
        out_dir.mkdir(parents=True, exist_ok=True)
        log_path = out_dir / "battery_sim.log"

        def _log(msg: str) -> None:
            text = f"[BatterySimulationPiece] {msg}"
            print(text, flush=True)
            with log_path.open("a", encoding="utf-8") as f:
                f.write(text + "\n")

        _log(f"Input load_csv={csv_path}")
        _log(f"Input scenario_yaml={scenario_path}")
        _log(f"Input virtual_solar_csv={solar_path}")
        if not str(csv_path).strip():
            raise ValueError("load_csv is empty — check upstream UserInputPiece wiring in Domino workflow")
        if not str(scenario_path).strip():
            raise ValueError("scenario_yaml is empty — check upstream SizingOptimizationPiece wiring")
        if not str(solar_path).strip():
            raise ValueError("virtual_solar_csv is empty — check upstream SolarSimulationPiece wiring")
        if not csv_path.is_file():
            raise FileNotFoundError(f"Load CSV not found: {csv_path}")
        if not scenario_path.is_file():
            raise FileNotFoundError(f"Scenario YAML not found: {scenario_path}")
        if not solar_path.is_file():
            raise FileNotFoundError(f"Virtual solar CSV not found: {solar_path}")

        try:
            sim = _load_simulate_module()
            cfg = yaml.safe_load(scenario_path.read_text(encoding="utf-8")) or {}
            load_df = sim.load_consumption_csv(csv_path)
            solar_df = pd.read_csv(solar_path)
            merged = _align_load_and_solar(load_df, solar_df)
            if len(merged) != len(load_df):
                _log(
                    f"WARNING: aligned {len(merged)} rows (load had {len(load_df)}, "
                    f"solar had {len(solar_df)})"
                )
            load_df = load_df.copy()
            load_df["datetime"] = pd.to_datetime(load_df["datetime"])
            df = (
                load_df.merge(merged[["datetime"]], on="datetime", how="inner")
                .sort_values("datetime")
                .reset_index(drop=True)
            )
            merged = merged.sort_values("datetime").reset_index(drop=True)

            bat = cfg.get("battery") or {}
            energy_kwh = float(bat.get("energy_kwh", 0.0))
            initial_soc = float(bat.get("initial_soc_pct", 50.0))

            if energy_kwh <= 1e-6:
                _log("battery.energy_kwh is 0; skipping dispatch and writing flat SOC profile")
                out_df = pd.DataFrame({"datetime": merged["datetime"], "soc_pct": initial_soc})
                cycles = 0.0
                throughput_mwh = 0.0
            else:
                dt_h = sim.infer_timestep_hours(df)
                price = sim.build_price_series(df, cfg).values.astype(float)
                net = merged["load_kw"].astype(float).values - merged["pv_kw"].astype(float).values
                if len(net) != len(price):
                    raise ValueError(
                        f"Aligned load/solar rows ({len(net)}) != price series length ({len(price)})"
                    )

                mrk = cfg.get("mrk") or {}
                en = cfg.get("energy") or {}
                _g, soc, _pb, _exp = sim.dispatch_battery(
                    net_load_kw=net,
                    price=price,
                    dt_h=dt_h,
                    energy_kwh=energy_kwh,
                    max_c_rate=float(bat.get("max_c_rate", 0.5)),
                    eta_c=float(bat.get("charge_efficiency", 0.95)),
                    eta_d=float(bat.get("discharge_efficiency", 0.95)),
                    initial_soc_pct=initial_soc,
                    mrk_contract_kw=float(mrk.get("contract_kw", 0.0)),
                    feed_in_eur_per_kwh=float(en.get("feed_in_surplus_eur_per_kwh", 0.05)),
                    pv_lcoe_eur_per_kwh=0.12,
                    battery_throughput_eur_per_kwh=0.02,
                    max_fraction_from_grid_charge=float(
                        bat.get("max_fraction_capacity_from_grid_charge", 0.72)
                    ),
                )
                out_df = pd.DataFrame({"datetime": merged["datetime"], "soc_pct": soc})
                cycles = float(sim.equivalent_full_cycles(pd.Series(soc)))
                throughput_mwh = float(
                    (pd.Series(soc).diff().abs().fillna(0.0).sum() / 100.0) * energy_kwh / 1000.0
                )

            summary_df = pd.DataFrame(
                [
                    {
                        "capacity_kWh": energy_kwh,
                        "cycles_equivalent": round(cycles, 4),
                        "energy_throughput_MWh": round(throughput_mwh, 4),
                    }
                ]
            )
            _log(f"Computed battery SOC rows={len(out_df)}")
        except Exception as exc:
            (out_dir / "battery_sim_error.txt").write_text(traceback.format_exc(), encoding="utf-8")
            _log(f"ERROR during battery simulation: {exc}")
            raise

        out_csv = out_dir / "virtual_battery_soc.csv"
        summary_csv = out_dir / "battery_summary.csv"
        out_df.to_csv(out_csv, index=False)
        summary_df.to_csv(summary_csv, index=False)
        _log(f"Wrote outputs: {out_csv}, {summary_csv}")
        return OutputModel(
            message="Battery simulation finished",
            virtual_battery_soc_csv=str(out_csv),
            battery_summary_csv=str(summary_csv),
        )
