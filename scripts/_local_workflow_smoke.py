"""One-off local MRK pipeline smoke (not for CI)."""
from __future__ import annotations

import shutil
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TESTS = ROOT / "tests"
USER_IN = ROOT / "user_input"
SRC_USER = Path(r"C:\Users\NTB\Domino\industry_sg_vre_workflow\pitonak_mrk\user_input")

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from domino.schemas.deploy_mode import DeployModeType

_DEPLOY = DeployModeType.local


def _piece(cls):
    return cls(deploy_mode=_DEPLOY, task_id="local-smoke", dag_id="local-smoke")

OUT = {
    "user": TESTS / "UserInputPiece_Output",
    "catalog": TESTS / "CatalogSyncPiece_Outputs",
    "tl": TESTS / "TechnicalLimitsPiece_Output",
    "sizing": TESTS / "SizingOptimizationPiece_Output",
    "ranker": TESTS / "CatalogRankerPiece_Outputs",
    "solar": TESTS / "SolarSimPiece_Outputs",
    "bstrategy": TESTS / "BatteryStrategyOptimizerPiece_Outputs",
    "battery": TESTS / "BatterySimPiece_Outputs",
    "simulate": TESTS / "SimulatePiece_Outputs",
    "kpi": TESTS / "KPIPiece_Outputs",
    "invest": TESTS / "InvestmentEvalPiece_Outputs",
    "dash": TESTS / "DashboardPiece_Outputs",
}


def run_step(label: str, fn):
    print(f"START {label}", flush=True)
    t0 = time.perf_counter()
    out = fn()
    print(f"DONE  {label} ({time.perf_counter() - t0:.2f}s)", flush=True)
    return out


def main() -> int:
    USER_IN.mkdir(parents=True, exist_ok=True)
    if SRC_USER.is_dir():
        for name in ("load_and_prices.csv", "scenario.yaml", "prices.csv", "load.csv"):
            src = SRC_USER / name
            if src.is_file():
                shutil.copy2(src, USER_IN / name)

    load_src = USER_IN / "load_and_prices.csv"
    scenario_src = USER_IN / "scenario.yaml"
    if not load_src.is_file() or not scenario_src.is_file():
        print("Missing user_input fixtures")
        return 1

    for d in OUT.values():
        d.mkdir(parents=True, exist_ok=True)

    from pieces.UserInputPiece.models import InputModel as UIIn
    from pieces.UserInputPiece.piece import UserInputPiece

    def s1():
        p = _piece(UserInputPiece)
        p.results_path = str(OUT["user"])
        prices = USER_IN / "prices.csv"
        o = p.piece_function(
            UIIn(
                load_csv=str(load_src),
                prices_csv=str(prices) if prices.is_file() else "",
                scenario_yaml=str(scenario_src),
            )
        )
        return o.load_csv, o.scenario_yaml

    load_csv, scenario_yaml = run_step("UserInput", s1)

    from pieces.CatalogSyncPiece.models import InputModel as CSIn
    from pieces.CatalogSyncPiece.piece import CatalogSyncPiece

    def s2():
        p = _piece(CatalogSyncPiece)
        p.results_path = str(OUT["catalog"])
        o = p.piece_function(CSIn(scenario_yaml=scenario_yaml))
        return o.pv_catalog_json, o.inverter_catalog_json, o.battery_catalog_json, o.catalog_manifest_json

    pv_cat, inv_cat, bat_cat, manifest = run_step("CatalogSync", s2)

    from pieces.TechnicalLimitsPiece.models import InputModel as TLIn
    from pieces.TechnicalLimitsPiece.piece import TechnicalLimitsPiece

    def s3():
        p = _piece(TechnicalLimitsPiece)
        p.results_path = str(OUT["tl"])
        o = p.piece_function(TLIn(load_csv=load_csv, scenario_yaml=scenario_yaml))
        return o.technical_limits_json, o.scenario_yaml

    tl_json, scenario_yaml = run_step("TechnicalLimits", s3)

    from pieces.SizingOptimizationPiece.models import InputModel as SZIn
    from pieces.SizingOptimizationPiece.piece import SizingOptimizationPiece

    def s4():
        p = _piece(SizingOptimizationPiece)
        p.results_path = str(OUT["sizing"])
        o = p.piece_function(SZIn(load_csv=load_csv, scenario_yaml=scenario_yaml, technical_limits_json=tl_json))
        return o.sized_scenario_yaml

    scenario_yaml = run_step("SizingOptimization", s4)

    from pieces.CatalogRankerPiece.models import InputModel as CRIn
    from pieces.CatalogRankerPiece.piece import CatalogRankerPiece

    def s5():
        p = _piece(CatalogRankerPiece)
        p.results_path = str(OUT["ranker"])
        o = p.piece_function(CRIn(scenario_yaml=scenario_yaml, pv_catalog_json=pv_cat))
        return o.catalog_ranked_recommendation_json

    ranked = run_step("CatalogRanker", s5)

    from pieces.SolarSimPiece.models import InputModel as SSIn
    from pieces.SolarSimPiece.piece import SolarSimPiece

    def s6():
        p = _piece(SolarSimPiece)
        p.results_path = str(OUT["solar"])
        o = p.piece_function(SSIn(load_csv=load_csv, scenario_yaml=scenario_yaml))
        return o.virtual_solar_csv

    solar_csv = run_step("SolarSim", s6)

    from pieces.BatteryStrategyOptimizerPiece.models import InputModel as BSIn
    from pieces.BatteryStrategyOptimizerPiece.piece import BatteryStrategyOptimizerPiece

    def s7():
        p = _piece(BatteryStrategyOptimizerPiece)
        p.results_path = str(OUT["bstrategy"])
        o = p.piece_function(BSIn(load_csv=load_csv, scenario_yaml=scenario_yaml))
        return o.battery_strategy_recommendation_json

    bstrat = run_step("BatteryStrategyOptimizer", s7)

    from pieces.BatterySimPiece.models import InputModel as BIn
    from pieces.BatterySimPiece.piece import BatterySimPiece

    def s8():
        p = _piece(BatterySimPiece)
        p.results_path = str(OUT["battery"])
        o = p.piece_function(
            BIn(
                load_csv=load_csv,
                scenario_yaml=scenario_yaml,
                virtual_solar_csv=solar_csv,
                battery_strategy_recommendation_json=bstrat,
            )
        )
        return o.virtual_battery_soc_csv, o.battery_summary_csv, o.battery_dispatch_csv

    _soc_csv, battery_summary_csv, battery_dispatch_csv = run_step("BatterySim", s8)

    from pieces.SimulatePiece.models import InputModel as SimIn
    from pieces.SimulatePiece.piece import SimulatePiece

    def s9():
        p = _piece(SimulatePiece)
        p.results_path = str(OUT["simulate"])
        o = p.piece_function(
            SimIn(
                load_csv=load_csv,
                scenario_yaml=scenario_yaml,
                virtual_solar_csv=solar_csv,
                battery_dispatch_csv=battery_dispatch_csv,
                battery_summary_csv=battery_summary_csv,
                ranked_catalog_json=ranked,
                inverter_catalog_json=inv_cat,
                battery_catalog_json=bat_cat,
                catalog_manifest_json=manifest,
            )
        )
        return o.report_json

    report_json = run_step("Simulate", s9)

    from pieces.KPIPiece.models import InputModel as KPIIn
    from pieces.KPIPiece.piece import KPIPiece

    def s10():
        p = _piece(KPIPiece)
        p.results_path = str(OUT["kpi"])
        o = p.piece_function(KPIIn(report_json=report_json))
        return o.kpi_results_csv

    kpi_csv = run_step("KPI", s10)

    from pieces.InvestmentEvalPiece.models import InputModel as IEIn
    from pieces.InvestmentEvalPiece.piece import InvestmentEvalPiece

    def s11():
        p = _piece(InvestmentEvalPiece)
        p.results_path = str(OUT["invest"])
        o = p.piece_function(IEIn(report_json=report_json, kpi_results_csv=kpi_csv))
        return o.investment_evaluation_csv

    inv_csv = run_step("InvestmentEval", s11)

    from pieces.DashboardPiece.models import InputModel as DIn
    from pieces.DashboardPiece.piece import DashboardPiece

    def s12():
        p = _piece(DashboardPiece)
        p.results_path = str(OUT["dash"])
        o = p.piece_function(DIn(report_json=report_json, kpi_results_csv=kpi_csv, investment_evaluation_csv=inv_csv))
        return o.dashboard_data_json

    dash = run_step("Dashboard", s12)

    checks = [
        OUT["simulate"] / "mrk_savings_report.json",
        OUT["simulate"] / "simulate.log",
        Path(kpi_csv),
        Path(inv_csv),
        Path(dash),
    ]
    missing = [p for p in checks if not p.is_file()]
    if missing:
        print("FAIL missing outputs:", *missing, sep="\n  ")
        return 1
    print("OK all key outputs present")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
