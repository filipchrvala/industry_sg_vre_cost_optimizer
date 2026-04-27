from __future__ import annotations

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
TESTS_DIR = PROJECT_ROOT / "tests"

STAGING = TESTS_DIR / "_staging"
OUT_USER_INPUT = TESTS_DIR / "UserInputPiece_Output"
OUT_CATALOG_SYNC = TESTS_DIR / "CatalogSyncPiece_Outputs"
OUT_CATALOG_RANKER = TESTS_DIR / "CatalogRankerPiece_Outputs"
OUT_TECHNICAL_LIMITS = TESTS_DIR / "TechnicalLimitsPiece_Output"
OUT_SIZING_OPT = TESTS_DIR / "SizingOptimizationPiece_Output"
OUT_SOLAR = TESTS_DIR / "SolarSimPiece_Outputs"
OUT_BATTERY_STRATEGY = TESTS_DIR / "BatteryStrategyOptimizerPiece_Outputs"
OUT_BATTERY = TESTS_DIR / "BatterySimPiece_Outputs"
OUT_SIMULATE = TESTS_DIR / "SimulatePiece_Outputs"
OUT_KPI = TESTS_DIR / "KPIPiece_Outputs"
OUT_INVESTMENT_EVAL = TESTS_DIR / "InvestmentEvalPiece_Outputs"
OUT_DASHBOARD = TESTS_DIR / "DashboardPiece_Outputs"

INPUT_LOAD_CSV = PROJECT_ROOT / "user_input" / "load_and_prices.csv"
INPUT_PRICES_CSV = PROJECT_ROOT / "user_input" / "prices.csv"
INPUT_SCENARIO_YAML = PROJECT_ROOT / "user_input" / "scenario.yaml"

REPORT_JSON = OUT_SIMULATE / "mrk_savings_report.json"
KPI_CSV = OUT_KPI / "kpi_results.csv"
INVESTMENT_EVAL_CSV = OUT_INVESTMENT_EVAL / "investment_evaluation.csv"
DASHBOARD_JSON = OUT_DASHBOARD / "dashboard_data.json"
TECHNICAL_LIMITS_JSON = OUT_TECHNICAL_LIMITS / "technical_limits.json"
SIZED_SCENARIO_YAML = OUT_SIZING_OPT / "scenario_sized.yaml"
VIRTUAL_SOLAR_CSV = OUT_SOLAR / "virtual_solar.csv"
BATTERY_STRATEGY_JSON = OUT_BATTERY_STRATEGY / "battery_strategy_recommendation.json"
VIRTUAL_BATTERY_SOC_CSV = OUT_BATTERY / "virtual_battery_soc.csv"
PV_CATALOG_JSON = OUT_CATALOG_SYNC / "pv_modules_online.json"
INVERTER_CATALOG_JSON = OUT_CATALOG_SYNC / "inverters_online.json"
CATALOG_MANIFEST_JSON = OUT_CATALOG_SYNC / "catalog_manifest.json"
CATALOG_RANKED_JSON = OUT_CATALOG_RANKER / "catalog_ranked_recommendation.json"
