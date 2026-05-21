from __future__ import annotations

import sys
import traceback
from pathlib import Path

import numpy as np
import pandas as pd
import yaml
from domino.base_piece import BasePiece

from .models import InputModel, OutputModel


def _load_consumption_csv(path: Path | str) -> pd.DataFrame:
    p = Path(path)
    if not p.is_file():
        raise FileNotFoundError(str(p))
    df = pd.read_csv(p, sep=None, engine="python", encoding="utf-8-sig")
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    if "datetime" not in df.columns:
        raise ValueError("CSV must contain column: datetime")
    if "load_kw" not in df.columns:
        if "load_mw" in df.columns:
            df["load_kw"] = pd.to_numeric(df["load_mw"], errors="coerce") * 1000.0
        else:
            raise ValueError("CSV must contain load_kw (or load_mw)")
    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    df = df.dropna(subset=["datetime"])
    df["load_kw"] = pd.to_numeric(df["load_kw"], errors="coerce").fillna(0.0).clip(lower=0.0)
    df = df.sort_values("datetime").reset_index(drop=True)
    if "price_eur_per_kwh" in df.columns:
        df["price_eur_per_kwh"] = pd.to_numeric(df["price_eur_per_kwh"], errors="coerce")
    else:
        df["price_eur_per_kwh"] = None
    return df


def _infer_timestep_hours(df: pd.DataFrame) -> float:
    if len(df) < 2:
        return 0.25
    d = df["datetime"].diff().dt.total_seconds().median() / 3600.0
    return float(d) if pd.notna(d) and d > 0 else 0.25


def _build_price_series(df: pd.DataFrame) -> pd.Series:
    if "price_eur_per_kwh" not in df.columns:
        raise ValueError("CSV must contain mandatory column: price_eur_per_kwh")
    s = pd.to_numeric(df["price_eur_per_kwh"], errors="coerce")
    if not s.notna().any():
        raise ValueError("CSV price_eur_per_kwh is mandatory and must contain at least one numeric value")
    med = float(s.median())
    return s.fillna(med).astype(float)


def _dis_cap_kw(soc_pct: float, e_kwh: float, eta_d: float, dt_h: float, pmax: float) -> float:
    e_avail = max(0.0, soc_pct / 100.0 * e_kwh * eta_d)
    return float(min(pmax, e_avail / dt_h if dt_h > 0 else 0.0))


def _ch_cap_kw(soc_pct: float, e_kwh: float, eta_c: float, dt_h: float, pmax: float) -> float:
    room = max(0.0, (100.0 - soc_pct) / 100.0 * e_kwh)
    return float(min(pmax, room / (dt_h * eta_c) if dt_h > 0 else 0.0))


def _dispatch_battery(
    net_load_kw: np.ndarray,
    price: np.ndarray,
    dt_h: float,
    *,
    energy_kwh: float,
    max_c_rate: float,
    eta_c: float,
    eta_d: float,
    initial_soc_pct: float,
    mrk_contract_kw: float,
    feed_in_eur_per_kwh: float = 0.05,
    pv_lcoe_eur_per_kwh: float = 0.12,
    battery_throughput_eur_per_kwh: float = 0.02,
    max_fraction_from_grid_charge: float = 0.72,
    excess_penalty_eur_per_kw: float = 0.0,
    peak_shaving_reserve_pct: float = 30.0,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """Battery dispatch aligned with SimulateMRKScenarioPiece logic (in-piece copy)."""
    n = len(net_load_kw)
    pmax = max(0.0, max_c_rate * energy_kwh)
    grid = np.zeros(n)
    export = np.zeros(n)
    soc_pct = np.zeros(n + 1)
    soc_pct[0] = float(initial_soc_pct)
    p_batt = np.zeros(n)

    p = np.asarray(price, dtype=float)
    p_low = float(np.quantile(p, 0.30))
    p_high = float(np.quantile(p, 0.75))
    p_exp = float(np.percentile(p, 70.0))
    p_med = float(np.median(p))
    net_pos = np.maximum(np.asarray(net_load_kw, dtype=float), 0.0)
    charge_ceiling_kw = float(np.quantile(net_pos, 0.85)) if len(net_pos) else 0.0
    if mrk_contract_kw > 0.0:
        charge_ceiling_kw = min(charge_ceiling_kw, mrk_contract_kw)

    e = float(energy_kwh)
    soc_kwh0 = float(initial_soc_pct) / 100.0 * e
    spv = soc_kwh0 * 0.65
    sg = min(soc_kwh0 * 0.35, max_fraction_from_grid_charge * e)
    spv = max(0.0, soc_kwh0 - sg)
    max_sg = max_fraction_from_grid_charge * e

    opp_kwh = max(feed_in_eur_per_kwh, pv_lcoe_eur_per_kwh) / max(eta_c, 1e-6)
    peak_value_per_kwh = float(excess_penalty_eur_per_kw) / max(dt_h, 1e-9)
    reserve_cost_per_kwh = opp_kwh + float(battery_throughput_eur_per_kwh)
    expected_mrk_overflow = bool(np.any(net_pos > mrk_contract_kw + 1e-6)) if mrk_contract_kw > 0.0 else False
    reserve_enabled = peak_value_per_kwh > reserve_cost_per_kwh and mrk_contract_kw > 0.0 and expected_mrk_overflow
    reserve_kwh = e * float(np.clip(peak_shaving_reserve_pct, 0.0, 95.0)) / 100.0 if reserve_enabled else 0.0
    value_eur = spv * opp_kwh + sg * (p_med / max(eta_c, 1e-6))

    def _total_kwh() -> float:
        return spv + sg

    def _s_pct() -> float:
        return 100.0 * _total_kwh() / max(e, 1e-9)

    for t in range(n):
        net = float(net_load_kw[t])
        pr = float(p[t])
        s = _s_pct()
        soc_k = _total_kwh()

        if e <= 1e-6:
            grid[t] = max(0.0, net)
            soc_pct[t + 1] = s
            continue

        if net <= 0.0:
            surplus = -net
            ch = min(surplus, _ch_cap_kw(s, e, eta_c, dt_h, pmax))
            kwh_in = ch * dt_h * eta_c
            room = max(0.0, e - spv - sg)
            add = min(kwh_in, room)
            spv += add
            value_eur += opp_kwh * add
            ch_eff = add / max(eta_c * dt_h, 1e-12) if add > 1e-12 else 0.0
            soc_pct[t + 1] = min(100.0, (spv + sg) / max(e, 1e-9) * 100.0)
            grid[t] = 0.0
            export[t] = max(0.0, surplus - ch_eff)
            p_batt[t] = -ch_eff
            continue

        dis_cap = _dis_cap_kw(s, e, eta_d, dt_h, pmax)
        over_mrk = max(0.0, net - mrk_contract_kw)
        avg_eur = value_eur / max(soc_k, 1e-9)
        thr = avg_eur / max(eta_d, 1e-9) + battery_throughput_eur_per_kwh
        above_reserve_kwh = max(0.0, soc_k - reserve_kwh)
        dis_cap_above_reserve = min(dis_cap, above_reserve_kwh * max(eta_d, 1e-9) / max(dt_h, 1e-9))

        want_dis = 0.0
        if over_mrk > 1e-6:
            want_dis = max(want_dis, min(over_mrk, dis_cap))
        if pr >= thr and soc_k > 1e-6:
            want_dis = max(want_dis, min(net * 0.55, dis_cap_above_reserve))
        if pr >= p_exp and soc_k > 0.08 * e:
            want_dis = max(want_dis, min(net * 0.45, dis_cap_above_reserve))

        dis = float(np.clip(want_dis, 0.0, min(dis_cap, net)))
        kwh_out = dis * dt_h / max(eta_d, 1e-9)
        tot_b = spv + sg
        if kwh_out > 1e-9 and tot_b > 1e-9:
            r = min(1.0, kwh_out / tot_b)
            value_eur *= 1.0 - r
            spv *= 1.0 - r
            sg *= 1.0 - r

        soc_k_after = spv + sg
        s_after = soc_k_after / e * 100.0

        ch = 0.0
        arbitrage_ok = (pr / max(eta_c, 1e-9) + battery_throughput_eur_per_kwh) < (
            p_high / max(eta_d, 1e-9) - 0.008
        )
        room_grid = max(0.0, max_sg - sg)
        room_total = max(0.0, e - spv - sg)
        need_reserve_refill = reserve_enabled and soc_k_after < reserve_kwh
        if pr <= p_low and s_after < 92.0 and (arbitrage_ok or need_reserve_refill) and room_grid > 1e-6:
            headroom = max(0.0, charge_ceiling_kw - (net - dis)) if charge_ceiling_kw > 0.0 else pmax
            if charge_ceiling_kw > 0.0 and headroom > 1e-6:
                ch = min(_ch_cap_kw(s_after, e, eta_c, dt_h, pmax), pmax * 0.9, headroom)
            elif charge_ceiling_kw <= 0.0:
                ch = min(_ch_cap_kw(s_after, e, eta_c, dt_h, pmax), pmax * 0.9)

        kwh_ch = ch * dt_h * eta_c
        kwh_ch = min(kwh_ch, room_total, room_grid)
        ch_grid = kwh_ch / max(eta_c * dt_h, 1e-12) if kwh_ch > 1e-12 else 0.0
        sg += kwh_ch
        value_eur += pr * ch_grid * dt_h

        grid[t] = max(0.0, net - dis + ch_grid)
        export[t] = 0.0
        soc_k_final = spv + sg
        soc_pct[t + 1] = float(np.clip(soc_k_final / e * 100.0, 0.0, 100.0))
        p_batt[t] = dis - ch_grid

    return grid, soc_pct[1:], p_batt, export


def _equivalent_full_cycles(soc_pct_series: pd.Series) -> float:
    return float(soc_pct_series.diff().abs().sum() / 200.0)


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
            raise ValueError("load_csv is empty — check upstream UserInputPiece wiring")
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
            cfg = yaml.safe_load(scenario_path.read_text(encoding="utf-8")) or {}
            load_df = _load_consumption_csv(csv_path)
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
                dt_h = _infer_timestep_hours(df)
                price = _build_price_series(df).values.astype(float)
                net = merged["load_kw"].astype(float).values - merged["pv_kw"].astype(float).values
                if len(net) != len(price):
                    raise ValueError(
                        f"Aligned load/solar rows ({len(net)}) != price series length ({len(price)})"
                    )

                mrk = cfg.get("mrk") or {}
                en = cfg.get("energy") or {}
                _g, soc, _pb, _exp = _dispatch_battery(
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
                cycles = float(_equivalent_full_cycles(pd.Series(soc)))
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
        except Exception:
            (out_dir / "battery_sim_error.txt").write_text(traceback.format_exc(), encoding="utf-8")
            _log(f"ERROR during battery simulation:\n{traceback.format_exc()}")
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
