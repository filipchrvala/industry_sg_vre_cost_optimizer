"""The local web form must emit the same scenario keys the pieces already read."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "webapp"))

from scenario_from_form import build_scenario  # noqa: E402


def test_demo_form_matches_expected_site() -> None:
    scenario = build_scenario(
        {
            "site_name": "Demo Industrial Site — Nitra",
            "latitude": "48.3061",
            "longitude": "18.0764",
            "require_battery": "false",
            "system_scope": "pv_and_battery",
        }
    )
    assert scenario["site"]["latitude"] == 48.3061
    assert scenario["equipment"]["auto"]["require_battery"] is False
    assert scenario["equipment"]["auto"]["min_battery_kwh"] == 0.0
    assert scenario["use_pv"] is True
    assert scenario["use_battery"] is True


def test_required_battery_uses_form_minimum() -> None:
    scenario = build_scenario({"require_battery": "true", "min_battery_kwh": "250"})
    assert scenario["equipment"]["auto"]["require_battery"] is True
    assert scenario["equipment"]["auto"]["min_battery_kwh"] == 250.0


def test_pv_only_disables_battery_flag() -> None:
    scenario = build_scenario({"system_scope": "pv_only"})
    assert scenario["use_pv"] is True
    assert scenario["use_battery"] is False
