"""IRR and the uncertainty percentiles have to be real calculations, not labels."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pieces.SimulatePiece.piece import irr, build_uncertainty_assessment  # noqa: E402


class TestIRR:
    def test_known_project(self):
        # Invest 100, recover 60 a year for two years: IRR is 13.07%.
        rate = irr([-100.0, 60.0, 60.0])
        assert rate == pytest.approx(0.1307, abs=0.001)

    def test_never_recovers_is_deeply_negative(self):
        rate = irr([-100.0, 10.0, 10.0])
        assert rate is not None
        assert rate < -0.5

    def test_all_outflows_have_no_rate(self):
        assert irr([-100.0, -10.0, -10.0]) is None


class TestUncertainty:
    def test_percentiles_come_from_a_distribution(self):
        bundle = {
            "baseline": {"total_operating_eur": 200_000.0},
            "days_in_sample": 365.0,
            "pv_capex": 400_000.0,
            "battery_capex": 100_000.0,
            "years": 15,
            "discount_rate": 0.075,
        }
        optimized = {"total_operating_eur": 80_000.0}
        out = build_uncertainty_assessment(bundle, optimized=optimized, iterations=2000)

        assert out["method"] == "monte_carlo_v1"
        assert out["p90_annual_savings_eur"] < out["p50_annual_savings_eur"] < out["p10_annual_savings_eur"]
        assert 0.0 < out["probability_npv_positive"] <= 1.0
        assert out["irr_pct"] is not None
        # A 120k annual saving on 500k CAPEX for 15 years is a healthy project.
        assert out["irr_pct"] > 15.0
