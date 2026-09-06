"""The consumption chart must compare grid energy, not 15-minute means."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from pieces.DashboardPiece.piece import DashboardPiece


def test_daily_and_monthly_are_sums(tmp_path: Path):
    index = pd.date_range("2024-01-01", periods=96 * 2, freq="15min")  # two days
    # 10 kWh every 15 minutes without the plant, 4 kWh with it.
    frame = pd.DataFrame(
        {
            "datetime": index,
            "baseline_energy_kwh_interval": 10.0,
            "optimized_energy_kwh_interval": 4.0,
        }
    )
    path = tmp_path / "profile.csv"
    frame.to_csv(path, index=False)

    out = DashboardPiece._build_consumption(path, tmp_path / "missing.csv")
    assert out["daily"]["without"] == [960.0, 960.0]
    assert out["daily"]["with"] == [384.0, 384.0]
    assert out["monthly"]["without"] == [1920.0]
    assert out["totals"]["saved_kwh"] == 1152.0
    assert out["totals"]["saved_pct"] == 60.0


def test_dispatch_fallback(tmp_path: Path):
    index = pd.date_range("2024-06-01", periods=4, freq="15min")
    frame = pd.DataFrame(
        {
            "datetime": index,
            "load_kw": 100.0,
            "pv_battery_grid_kw": 40.0,
        }
    )
    path = tmp_path / "dispatch.csv"
    frame.to_csv(path, index=False)
    out = DashboardPiece._build_consumption(tmp_path / "missing.csv", path)
    # 100 kW * 0.25 h * 4 = 100 kWh without; 40 kW * 0.25 * 4 = 40 kWh with.
    assert out["totals"]["without_kwh"] == 100.0
    assert out["totals"]["with_kwh"] == 40.0
