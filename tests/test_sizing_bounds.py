"""Guard the layout constants that translate site area into installable capacity.

These two numbers decide the upper bound of the whole size search. If one is
wrong the optimiser does not fail, it quietly returns a smaller system, and the
customer sees a plausible report recommending the wrong investment. The battery
figure shipped at 0.25 kWh/m², which capped every site with a stated battery
area at a few tens of kWh and made storage structurally unrecommendable.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pieces.TechnicalLimitsPiece.piece import _technical_bounds_kwp_kwh  # noqa: E402


def load_profile(days: int = 7, step_minutes: int = 15) -> pd.DataFrame:
    index = pd.date_range("2024-06-01", periods=int(days * 24 * 60 / step_minutes), freq="15min")
    return pd.DataFrame({"datetime": index, "load_kw": 500.0})


class TestBatteryAreaDensity:
    def test_default_matches_real_hardware(self):
        """The default must be within reach of the products in the catalog."""
        catalog = json.loads(
            (ROOT / "catalog" / "battery_catalog.json").read_text(encoding="utf-8")
        )
        densities = [
            p["nominal_kwh"] / p["footprint_m2"]
            for p in catalog["products"]
            if p.get("footprint_m2")
        ]
        lowest = min(densities)

        df = load_profile()
        bounds = _technical_bounds_kwp_kwh(
            {"equipment": {"constraints": {"max_battery_area_m2": 100.0}}}, df, 0.25
        )
        implied = bounds["max_kwh"] / 100.0

        # Allocated site area is larger than device footprint because of access
        # aisles and fire separation, so the planning figure must sit below the
        # least dense product, but within the same order of magnitude.
        assert implied < lowest, (
            f"planning density {implied:.1f} kWh/m2 exceeds the least dense catalog "
            f"product at {lowest:.1f} kWh/m2"
        )
        assert implied >= lowest / 4, (
            f"planning density {implied:.1f} kWh/m2 is more than 4x below the least "
            f"dense catalog product at {lowest:.1f} kWh/m2; a site with a stated "
            f"battery area would be capped far under what fits"
        )

    def test_stated_area_permits_a_useful_battery(self):
        """A 160 m² battery room has to allow more than a token system."""
        df = load_profile()
        bounds = _technical_bounds_kwp_kwh(
            {"equipment": {"constraints": {"max_battery_area_m2": 160.0}}}, df, 0.25
        )
        assert bounds["max_kwh"] > 1000.0, (
            f"160 m2 yielded only {bounds['max_kwh']:.0f} kWh"
        )

    def test_hard_cap_still_wins(self):
        df = load_profile()
        bounds = _technical_bounds_kwp_kwh(
            {
                "equipment": {
                    "constraints": {"max_battery_area_m2": 160.0, "max_battery_kwh": 500.0}
                }
            },
            df,
            0.25,
        )
        assert bounds["max_kwh"] == pytest.approx(500.0)


class TestPvAreaDensity:
    def test_roof_density_is_physically_possible(self):
        """kWp per m² cannot exceed what a real module achieves."""
        catalog = json.loads(
            (ROOT / "catalog" / "pv_modules_catalog.json").read_text(encoding="utf-8")
        )
        best = max(
            m["power_wp"] / m["area_m2"] / 1000.0
            for m in catalog["modules"]
            if m.get("area_m2")
        )

        df = load_profile()
        bounds = _technical_bounds_kwp_kwh(
            {"equipment": {"constraints": {"max_roof_area_m2": 1000.0}}}, df, 0.25
        )
        implied = bounds["max_kwp"] / 1000.0
        assert implied <= best, (
            f"planning density {implied:.3f} kWp/m2 exceeds the densest catalog "
            f"module at {best:.3f} kWp/m2"
        )

    def test_capex_cap_narrows_both_axes(self):
        df = load_profile()
        cfg = {
            "pv": {"specific_capex_eur_per_kwp": 800.0},
            "battery": {"specific_capex_eur_per_kwh": 400.0},
            "equipment": {
                "constraints": {
                    "max_roof_area_m2": 10000.0,
                    "max_battery_area_m2": 500.0,
                    "max_capex_eur": 400_000.0,
                }
            },
        }
        bounds = _technical_bounds_kwp_kwh(cfg, df, 0.25)
        assert bounds["max_kwp"] <= 400_000.0 / 800.0 + 1e-6
        assert bounds["max_kwh"] <= 400_000.0 / 400.0 + 1e-6
