from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import yaml


def test_run_workflow_exits_zero_and_generates_outputs():
    root = Path(__file__).resolve().parent.parent
    user_input = root / "user_input"
    user_input.mkdir(parents=True, exist_ok=True)
    load_and_prices = user_input / "load_and_prices.csv"
    scenario = user_input / "scenario.yaml"
    if not load_and_prices.is_file():
        rows = ["datetime,load_kw,price_eur_per_kwh"]
        for h in range(24):
            for q in range(4):
                m = q * 15
                rows.append(f"2025-06-15 {h:02d}:{m:02d}:00,300,0.15")
        load_and_prices.write_text("\n".join(rows), encoding="utf-8")
    if not scenario.is_file():
        cfg = {
            "use_pv": True,
            "use_battery": True,
            "pv": {"installed_kwp": 200.0, "yield_kwh_per_kwp_year": 1000.0, "specific_capex_eur_per_kwp": 800.0},
            "battery": {"energy_kwh": 400.0, "max_c_rate": 0.5, "specific_capex_eur_per_kwh": 400.0},
            "mrk": {"contract_kw": 320.0, "fee_eur_per_kw_month": 4.5, "excess_peak_penalty_eur_per_kw": 32.0},
            "analysis": {"amortization_years": 12, "discount_rate": 0.08},
            "energy": {"feed_in_surplus_eur_per_kwh": 0.05},
        }
        scenario.write_text(yaml.safe_dump(cfg, allow_unicode=True), encoding="utf-8")

    r = subprocess.run([sys.executable, str(root / "run_workflow.py")], cwd=str(root))
    assert r.returncode == 0
    assert (root / "tests" / "SimulatePiece_Outputs" / "mrk_savings_report.json").is_file()
    assert (root / "tests" / "KPIPiece_Outputs" / "kpi_results.csv").is_file()
    assert (root / "tests" / "DashboardPiece_Outputs" / "dashboard_data.json").is_file()
    assert (root / "tests" / "UserInputPiece_Output" / "user_input_summary.json").is_file()
    assert (root / "tests" / "UserInputPiece_Output" / "user_input_validated.json").is_file()
