from __future__ import annotations

from pathlib import Path
import traceback

import numpy as np
import pandas as pd
import yaml
from domino.base_piece import BasePiece

from .models import InputModel, OutputModel

PIECE_BUILD = "0.1.19"


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [c.strip().lower().replace(" ", "_") for c in out.columns]
    return out


def _datetime_key(series: pd.Series) -> pd.Series:
    dt = pd.to_datetime(series, errors="coerce", utc=True)
    if hasattr(dt.dtype, "tz") and dt.dt.tz is not None:
        dt = dt.dt.tz_convert("UTC").dt.tz_localize(None)
    return dt


def _align_load_and_solar(load_df: pd.DataFrame, solar_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    sd = _normalize_columns(solar_df)
    if "pv_kw" not in sd.columns:
        raise ValueError("virtual_solar_csv must contain pv_kw column")
    ld = load_df.copy()
    ld["datetime"] = _datetime_key(ld["datetime"])
    sd["datetime"] = _datetime_key(sd["datetime"])
    ld = ld.dropna(subset=["datetime"])
    sd = sd.dropna(subset=["datetime"])
    merged = ld[["datetime", "load_kw"]].merge(sd[["datetime", "pv_kw"]], on="datetime", how="inner")
    if len(merged) == 0:
        raise ValueError(
            f"No overlapping datetimes between load and solar CSV (load={len(ld)}, solar={len(sd)})"
        )
    df = ld.merge(merged[["datetime"]], on="datetime", how="inner").sort_values("datetime").reset_index(drop=True)
    merged = merged.sort_values("datetime").reset_index(drop=True)
    return df, merged


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
    return s.fillna(float(s.median())).astype(float)


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
    feed_in_eur_per_kwh: float,
    max_fraction_from_grid_charge: float,
) -> np.ndarray:
    n = len(net_load_kw)
    pmax = max(0.0, max_c_rate * energy_kwh)
    soc_pct = np.zeros(n + 1)
    soc_pct[0] = float(initial_soc_pct)
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
    spv = max(0.0, soc_kwh0 * 0.65)
    sg = min(soc_kwh0 * 0.35, max_fraction_from_grid_charge * e)
    spv = max(0.0, soc_kwh0 - sg)
    max_sg = max_fraction_from_grid_charge * e
    opp_kwh = max(feed_in_eur_per_kwh, 0.12) / max(eta_c, 1e-6)
    value_eur = spv * opp_kwh + sg * (p_med / max(eta_c, 1e-6))

    for t in range(n):
        net = float(net_load_kw[t])
        pr = float(p[t])
        soc_k = spv + sg
        s = 100.0 * soc_k / max(e, 1e-9)

        if e <= 1e-6:
            soc_pct[t + 1] = s
            continue

        if net <= 0.0:
            surplus = -net
            ch = min(surplus, _ch_cap_kw(s, e, eta_c, dt_h, pmax))
            kwh_in = ch * dt_h * eta_c
            add = min(kwh_in, max(0.0, e - spv - sg))
            spv += add
            value_eur += opp_kwh * add
            soc_pct[t + 1] = min(100.0, (spv + sg) / max(e, 1e-9) * 100.0)
            continue

        dis_cap = _dis_cap_kw(s, e, eta_d, dt_h, pmax)
        over_mrk = max(0.0, net - mrk_contract_kw)
        thr = (value_eur / max(soc_k, 1e-9)) / max(eta_d, 1e-9) + 0.02
        want_dis = 0.0
        if over_mrk > 1e-6:
            want_dis = max(want_dis, min(over_mrk, dis_cap))
        if pr >= thr and soc_k > 1e-6:
            want_dis = max(want_dis, min(net * 0.55, dis_cap))
        if pr >= p_exp and soc_k > 0.08 * e:
            want_dis = max(want_dis, min(net * 0.45, dis_cap))
        dis = float(np.clip(want_dis, 0.0, min(dis_cap, net)))
        kwh_out = dis * dt_h / max(eta_d, 1e-9)
        tot_b = spv + sg
        if kwh_out > 1e-9 and tot_b > 1e-9:
            r = min(1.0, kwh_out / tot_b)
            value_eur *= 1.0 - r
            spv *= 1.0 - r
            sg *= 1.0 - r

        s_after = (spv + sg) / e * 100.0
        ch = 0.0
        if pr <= p_low and s_after < 92.0 and max(0.0, max_sg - sg) > 1e-6:
            headroom = max(0.0, charge_ceiling_kw - (net - dis)) if charge_ceiling_kw > 0.0 else pmax
            ch = min(_ch_cap_kw(s_after, e, eta_c, dt_h, pmax), pmax * 0.9, headroom)
        kwh_ch = min(ch * dt_h * eta_c, max(0.0, e - spv - sg), max(0.0, max_sg - sg))
        sg += kwh_ch
        soc_pct[t + 1] = float(np.clip((spv + sg) / e * 100.0, 0.0, 100.0))

    return soc_pct[1:]


class BatterySimulationPiece(BasePiece):
    """Battery SOC: datetime-aligned load/PV + inline MRK dispatch (no heavy Simulate import)."""

    def piece_function(self, input_data: InputModel) -> OutputModel:
        csv_path = Path(input_data.load_csv)
        scenario_path = Path(input_data.scenario_yaml)
        solar_path = Path(input_data.virtual_solar_csv)
        out_dir = Path(self.results_path) if self.results_path else csv_path.parent
        log_path = out_dir / "battery_sim.log"
        err_path = out_dir / "battery_sim_error.txt"
        started_path = out_dir / "battery_sim_started.txt"

        try:
            _write_text(
                started_path,
                f"piece_build={PIECE_BUILD}\nresults_path={self.results_path}\nout_dir={out_dir}\n",
            )
        except OSError as boot_exc:
            print(f"[BatterySimulationPiece] bootstrap write failed: {boot_exc}", flush=True)

        def _log(msg: str) -> None:
            text = f"[BatterySimulationPiece] {msg}"
            print(text, flush=True)
            try:
                with log_path.open("a", encoding="utf-8") as f:
                    f.write(text + "\n")
            except OSError:
                pass

        try:
            _log(f"Input load_csv={csv_path}")
            _log(f"Input scenario_yaml={scenario_path}")
            _log(f"Input virtual_solar_csv={solar_path}")
            if not csv_path.is_file():
                raise FileNotFoundError(f"Load CSV not found: {csv_path}")
            if not scenario_path.is_file():
                raise FileNotFoundError(f"Scenario YAML not found: {scenario_path}")
            if not solar_path.is_file():
                raise FileNotFoundError(f"Virtual solar CSV not found: {solar_path}")

            cfg = yaml.safe_load(scenario_path.read_text(encoding="utf-8")) or {}
            load_df = _load_consumption_csv(csv_path)
            solar_df = pd.read_csv(solar_path, sep=None, engine="python", encoding="utf-8-sig")
            df, merged = _align_load_and_solar(load_df, solar_df)
            _log(f"Aligned load/solar rows={len(merged)} (load={len(load_df)}, solar={len(solar_df)})")

            bat = cfg.get("battery") or {}
            energy_kwh = float(bat.get("energy_kwh", 0.0))
            initial_soc = float(bat.get("initial_soc_pct", 50.0))
            if energy_kwh <= 1e-6:
                soc = np.full(len(merged), initial_soc)
            else:
                dt_h = _infer_timestep_hours(df)
                price = _build_price_series(df).values.astype(float)
                net = merged["load_kw"].astype(float).values - merged["pv_kw"].astype(float).values
                mrk = cfg.get("mrk") or {}
                en = cfg.get("energy") or {}
                soc = _dispatch_battery(
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
                    max_fraction_from_grid_charge=float(
                        bat.get("max_fraction_capacity_from_grid_charge", 0.72)
                    ),
                )
            out_df = pd.DataFrame({"datetime": merged["datetime"], "soc_pct": soc})
            cycles = float(pd.Series(soc).diff().abs().sum() / 200.0)
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
        except Exception:
            _write_text(err_path, traceback.format_exc())
            _log("ERROR — see battery_sim_error.txt in task Results")
            raise
