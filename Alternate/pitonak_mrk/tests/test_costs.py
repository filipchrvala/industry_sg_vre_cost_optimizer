from __future__ import annotations

import pandas as pd

from pieces.SimulatePiece.piece import annual_capex_charge_eur, energy_cost_eur, mrk_component_monthly, synthetic_pv_kw


def test_energy_cost_simple():
    grid = pd.Series([10.0, 0.0, 5.0])
    price = pd.Series([0.2, 0.2, 0.3])
    assert energy_cost_eur(grid, price, dt_h=1.0) == 10 * 0.2 + 5 * 0.3


def test_mrk_two_months_fixed_and_excess():
    ts = pd.to_datetime(
        ["2025-01-15 12:00", "2025-01-20 12:00", "2025-02-10 12:00", "2025-02-11 12:00"]
    )
    g = pd.Series([100.0, 150.0, 80.0, 200.0])
    total, detail = mrk_component_monthly(
        g,
        ts,
        contract_kw=120.0,
        fee_eur_per_kw_month=5.0,
        excess_penalty_eur_per_kw=50.0,
    )
    assert total == 2 * (120 * 5) + 30 * 50 + 80 * 50
    assert "2025-01" in detail and "2025-02" in detail


def test_annuity_positive_rate():
    a = annual_capex_charge_eur(10000.0, 0.0, years=10, discount_rate=0.08)
    assert 1400 < a < 1600


def test_synthetic_pv_scales_to_sample_period():
    dt = pd.date_range("2025-06-15 00:00:00", periods=96, freq="15min")
    pv = synthetic_pv_kw(pd.Series(dt), installed_kwp=100.0, yield_kwh_per_kwp_year=1000.0)
    total_kwh_day = float((pv * 0.25).sum())
    expected_day_kwh = 100.0 * 1000.0 / 365.0
    assert 0.8 * expected_day_kwh <= total_kwh_day <= 1.2 * expected_day_kwh
