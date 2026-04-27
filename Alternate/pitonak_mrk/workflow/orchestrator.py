from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

from . import paths as P
from . import steps


def _run_step(step_no: int, total_steps: int, label: str, fn, *args):
    print(f"[{step_no}/{total_steps}] START {label}")
    t0 = time.perf_counter()
    out = fn(*args)
    elapsed = time.perf_counter() - t0
    print(f"[{step_no}/{total_steps}] DONE  {label} ({elapsed:.2f}s)")
    return out


def run_full_pipeline(root: Path | None = None) -> Path:
    root = root or P.PROJECT_ROOT
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))

    total = 12
    load_csv, scenario_yaml = _run_step(1, total, "UserInput", steps.step_user_input)
    pv_catalog_json, inverter_catalog_json, battery_catalog_json, _manifest = _run_step(
        2, total, "CatalogSync", steps.step_catalog_sync, scenario_yaml
    )
    tl_json, scenario_yaml = _run_step(
        3, total, "TechnicalLimits", steps.step_technical_limits, load_csv, scenario_yaml
    )
    scenario_yaml = _run_step(
        4, total, "SizingOptimization", steps.step_sizing_optimization, load_csv, scenario_yaml, tl_json
    )
    ranked_catalog_json = _run_step(5, total, "CatalogRanker", steps.step_catalog_ranker, scenario_yaml, pv_catalog_json)
    virtual_solar_csv = _run_step(6, total, "SolarSim", steps.step_solar, load_csv, scenario_yaml)
    _run_step(7, total, "BatteryStrategyOptimizer", steps.step_battery_strategy, load_csv, scenario_yaml)
    _run_step(8, total, "BatterySim", steps.step_battery, load_csv, scenario_yaml, virtual_solar_csv)
    report_json = _run_step(
        9,
        total,
        "Simulate",
        steps.step_simulate,
        load_csv,
        scenario_yaml,
        ranked_catalog_json,
        inverter_catalog_json,
        battery_catalog_json,
        _manifest,
    )
    kpi_csv = _run_step(10, total, "KPI", steps.step_kpi, report_json)
    inv_csv = _run_step(11, total, "InvestmentEval", steps.step_investment_eval, report_json, kpi_csv)
    dash_json = _run_step(12, total, "Dashboard", steps.step_dashboard, report_json, kpi_csv, inv_csv)
    return Path(dash_json)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Workflow: UserInput -> CatalogSync -> TechnicalLimits -> SizingOptimization -> CatalogRanker -> "
            "SolarSim -> BatteryStrategyOptimizer -> BatterySim -> Simulate -> KPI -> InvestmentEval -> Dashboard"
        )
    )
    parser.parse_args(argv)
    t0 = time.perf_counter()
    try:
        out = run_full_pipeline(P.PROJECT_ROOT)
    except Exception as exc:
        elapsed = time.perf_counter() - t0
        print("\n" + "=" * 60)
        print(f"WORKFLOW FAILED after {elapsed:.2f}s")
        print(f"Reason: {exc}")
        print("=" * 60)
        return 1

    elapsed = time.perf_counter() - t0
    print("\n" + "=" * 60)
    print(f"WORKFLOW OK in {elapsed:.2f}s")
    print(f"Dashboard: {out}")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
