from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
import yaml

from pieces.UserInputPiece.models import InputModel as UserInputModel
from pieces.UserInputPiece.piece import UserInputPiece
from pieces.SimulatePiece.piece import run_analysis


@pytest.fixture
def tiny_csv(tmp_path: Path) -> Path:
    """96 steps = 1 day @ 15 min."""
    lines = ["datetime,load_kw,price_eur_per_kwh"]
    for h in range(24):
        for q in range(4):
            m = q * 15
            load = 300.0 + 50.0 * (h >= 8 and h < 20)
            price = 0.12 if h < 7 or h > 22 else 0.28
            lines.append(f"2025-06-15 {h:02d}:{m:02d}:00,{load},{price}")
    p = tmp_path / "day.csv"
    p.write_text("\n".join(lines), encoding="utf-8")
    return p


@pytest.fixture
def tiny_scenario(tmp_path: Path) -> Path:
    cfg = {
        "use_pv": True,
        "use_battery": True,
        "pv": {"installed_kwp": 200.0, "yield_kwh_per_kwp_year": 1000.0, "specific_capex_eur_per_kwp": 800.0},
        "battery": {
            "energy_kwh": 400.0,
            "max_c_rate": 0.5,
            "specific_capex_eur_per_kwh": 400.0,
        },
        "mrk": {
            "contract_kw": 320.0,
            "fee_eur_per_kw_month": 4.5,
            "excess_peak_penalty_eur_per_kw": 32.0,
        },
        "analysis": {"amortization_years": 12, "discount_rate": 0.08},
        "energy": {"feed_in_surplus_eur_per_kwh": 0.05},
    }
    sp = tmp_path / "scenario.yaml"
    sp.write_text(yaml.safe_dump(cfg, allow_unicode=True), encoding="utf-8")
    return sp


def test_run_analysis_produces_scenarios(tiny_csv: Path, tiny_scenario: Path, tmp_path: Path):
    r = run_analysis(tiny_csv, tiny_scenario, output_dir=tmp_path / "out")
    assert "baseline" in r["scenarios"]
    assert r["scenarios"]["baseline"]["label"] == "baseline_no_storage"
    assert "mrk_cost_period_eur" in r["scenarios"]["baseline"]
    assert r["savings_vs_baseline"]["pv_and_battery"] is not None
    assert "executive_summary" in r and "operating_savings_eur_period" in r["executive_summary"]
    assert r.get("mrk_and_rv") is not None
    assert "input_quality" in r
    assert "equipment" in r and r["equipment"].get("resolved")
    assert r["scenarios"].get("optimized")
    assert (r.get("meta") or {}).get("schema_version") == "mrk_report_v2"
    assert "uncertainty_assessment" in r
    assert ((r.get("data_contracts") or {}).get("validation") or {}).get("status") in {"ok", "warning"}
    assert (tmp_path / "out" / "mrk_savings_report.json").is_file()


def test_run_analysis_requires_price_column(tmp_path: Path, tiny_scenario: Path):
    lines = ["datetime,load_kw"]
    for h in range(24):
        for q in range(4):
            m = q * 15
            lines.append(f"2025-06-15 {h:02d}:{m:02d}:00,320")
    bad_csv = tmp_path / "no_price.csv"
    bad_csv.write_text("\n".join(lines), encoding="utf-8")

    with pytest.raises(ValueError, match="price_eur_per_kwh"):
        run_analysis(bad_csv, tiny_scenario, output_dir=tmp_path / "out2")


def test_user_input_merges_separate_load_and_price_csv(tmp_path: Path):
    load = tmp_path / "consumption.csv"
    load.write_text(
        "\n".join(
            [
                "date time;prikon A;prikon B",
                "1.1.25 0:00;10,0;5,0",
                "1.1.25 0:15;11,0;4,0",
            ]
        ),
        encoding="utf-8",
    )
    prices = tmp_path / "prices.csv"
    prices.write_text(
        "\n".join(
            [
                "datetime,price_eur_kwh",
                "2025-01-01 00:00,0.15",
                "2025-01-01 00:15,0.18",
            ]
        ),
        encoding="utf-8",
    )
    scenario = tmp_path / "scenario.yaml"
    scenario.write_text("use_pv: false\nuse_battery: false\n", encoding="utf-8")

    piece = UserInputPiece()
    piece.results_path = str(tmp_path / "out")
    out = piece.piece_function(
        UserInputModel(load_csv=str(load), prices_csv=str(prices), scenario_yaml=str(scenario))
    )
    merged = Path(out.load_csv)
    assert merged.is_file()
    txt = merged.read_text(encoding="utf-8")
    assert "load_kw" in txt and "price_eur_per_kwh" in txt


def test_user_input_collapses_duplicate_timestamps(tmp_path: Path):
    load = tmp_path / "load_and_price_dupe.csv"
    load.write_text(
        "\n".join(
            [
                "datetime,load_kw,price_eur_per_kwh",
                "2025-01-01 00:00,10,0.2",
                "2025-01-01 00:00,15,0.4",
                "2025-01-01 00:15,20,0.3",
            ]
        ),
        encoding="utf-8",
    )
    scenario = tmp_path / "scenario.yaml"
    scenario.write_text("use_pv: false\nuse_battery: false\n", encoding="utf-8")
    out_dir = tmp_path / "out_dupe"
    piece = UserInputPiece()
    piece.results_path = str(out_dir)
    out = piece.piece_function(
        UserInputModel(load_csv=str(load), prices_csv="", scenario_yaml=str(scenario))
    )
    merged = Path(out.load_csv)
    df = pd.read_csv(merged)
    assert len(df) == 2
    assert abs(float(df.loc[0, "load_kw"]) - 25.0) < 1e-9
    assert abs(float(df.loc[0, "price_eur_per_kwh"]) - 0.3) < 1e-9


def test_user_input_repairs_missing_intervals(tmp_path: Path):
    load = tmp_path / "load_and_price_gaps.csv"
    load.write_text(
        "\n".join(
            [
                "datetime,load_kw,price_eur_per_kwh",
                "2025-01-01 00:00,10,0.2",
                "2025-01-01 00:30,30,0.4",
            ]
        ),
        encoding="utf-8",
    )
    scenario = tmp_path / "scenario.yaml"
    scenario.write_text(
        "\n".join(
            [
                "use_pv: false",
                "use_battery: false",
                "timestep_minutes: 15",
                "production:",
                "  gap_repair_enabled: true",
            ]
        ),
        encoding="utf-8",
    )
    out_dir = tmp_path / "out_gaps"
    piece = UserInputPiece()
    piece.results_path = str(out_dir)
    out = piece.piece_function(
        UserInputModel(load_csv=str(load), prices_csv="", scenario_yaml=str(scenario))
    )
    df = pd.read_csv(Path(out.load_csv))
    assert len(df) == 3
    # Missing 00:15 gets linearly interpolated.
    assert abs(float(df.loc[1, "load_kw"]) - 20.0) < 1e-9
    assert abs(float(df.loc[1, "price_eur_per_kwh"]) - 0.3) < 1e-9
