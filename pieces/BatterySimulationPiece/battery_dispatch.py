"""
Battery dispatch helpers for BatterySimulationPiece only.

Domino runs each piece in an isolated task — importing the full SimulateMRKScenarioPiece
module (~1800 lines + heavy imports) can OOM or fail before logging. This module keeps
the same algorithms without pulling the MRK scenario piece class.
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd


def load_consumption_csv(path: Path | str) -> pd.DataFrame:
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


def infer_timestep_hours(df: pd.DataFrame) -> float:
    if len(df) < 2:
        return 0.25
    d = df["datetime"].diff().dt.total_seconds().median() / 3600.0
    return float(d) if pd.notna(d) and d > 0 else 0.25


def build_price_series(df: pd.DataFrame, cfg: dict) -> pd.Series:
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


def dispatch_battery(
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
    price_low: float | None = None,
    price_high: float | None = None,
    price_expensive: float | None = None,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    n = len(net_load_kw)
    pmax = max(0.0, max_c_rate * energy_kwh)
    grid = np.zeros(n)
    export = np.zeros(n)
    soc_pct = np.zeros(n + 1)
    soc_pct[0] = float(initial_soc_pct)
    p_batt = np.zeros(n)

    p = np.asarray(price, dtype=float)
    p_low = float(price_low) if price_low is not None else float(np.quantile(p, 0.30))
    p_high = float(price_high) if price_high is not None else float(np.quantile(p, 0.75))
    p_exp = float(price_expensive) if price_expensive is not None else float(np.percentile(p, 70.0))
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
        tot = _total_kwh()
        return 100.0 * tot / max(e, 1e-9)

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
        thr = avg_eur / max(eta_d, 1e-6) + battery_throughput_eur_per_kwh
        above_reserve_kwh = max(0.0, soc_k - reserve_kwh)
        dis_cap_above_reserve = min(dis_cap, above_reserve_kwh * max(eta_d, 1e-6) / max(dt_h, 1e-9))

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
        arbitrage_ok = (pr / max(eta_c, 1e-6) + battery_throughput_eur_per_kwh) < (p_high / max(eta_d, 1e-6) - 0.008)
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


def equivalent_full_cycles(soc_pct_series: pd.Series) -> float:
    ch = soc_pct_series.diff().abs().sum()
    return float(ch / 200.0)


def read_solar_pv_kw(solar_path: Path, load_df: pd.DataFrame) -> np.ndarray:
    """Align virtual_solar.csv to load timestamps (Domino upstream paths may differ in row order)."""
    solar_df = pd.read_csv(solar_path, sep=None, engine="python", encoding="utf-8-sig")
    solar_df.columns = [c.strip().lower().replace(" ", "_") for c in solar_df.columns]
    if "pv_kw" not in solar_df.columns:
        raise ValueError("virtual_solar_csv must contain pv_kw column")
    if "datetime" not in solar_df.columns:
        if isinstance(solar_df.index, pd.DatetimeIndex):
            solar_df = solar_df.reset_index().rename(columns={"index": "datetime"})
        elif solar_df.index.name and str(solar_df.index.name).lower().replace(" ", "_") == "datetime":
            solar_df = solar_df.reset_index()
        else:
            if len(solar_df) == len(load_df):
                return solar_df["pv_kw"].astype(float).values
            raise ValueError("virtual_solar_csv must contain datetime column for merge with load_csv")
    solar_df["datetime"] = pd.to_datetime(solar_df["datetime"], errors="coerce")
    merged = load_df[["datetime"]].merge(
        solar_df[["datetime", "pv_kw"]],
        on="datetime",
        how="left",
    )
    if merged["pv_kw"].isna().any():
        if len(solar_df) == len(load_df):
            return solar_df["pv_kw"].astype(float).values
        missing = int(merged["pv_kw"].isna().sum())
        raise ValueError(
            f"virtual_solar_csv has {missing} timestamps not matching load_csv; "
            "re-run SolarSimulationPiece on the same load_csv"
        )
    return merged["pv_kw"].astype(float).values
