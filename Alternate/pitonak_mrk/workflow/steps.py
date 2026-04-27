from __future__ import annotations

from . import paths as P


def step_user_input() -> tuple[str, str]:
    from pieces.UserInputPiece.models import InputModel
    from pieces.UserInputPiece.piece import UserInputPiece

    P.OUT_USER_INPUT.mkdir(parents=True, exist_ok=True)
    piece = UserInputPiece()
    piece.results_path = str(P.OUT_USER_INPUT)
    user_input_dir = P.PROJECT_ROOT / "user_input"
    preferred_load = user_input_dir / "load_csv.csv"
    if preferred_load.is_file():
        # Prefer explicit load-only input when available (pairs with prices.csv).
        load_csv = preferred_load
    elif P.INPUT_LOAD_CSV.is_file():
        load_csv = P.INPUT_LOAD_CSV
    else:
        candidates = [
            p
            for p in sorted(user_input_dir.glob("*.csv"))
            if p.name.lower() not in {"prices.csv", "load_and_prices.csv"}
        ]
        if not candidates:
            fallback_candidates = [
                p for p in sorted(user_input_dir.glob("*.csv")) if p.name.lower() != "prices.csv"
            ]
            if not fallback_candidates:
                raise FileNotFoundError(
                    f"No load CSV found. Expected {preferred_load} or {P.INPUT_LOAD_CSV} or any *.csv in {user_input_dir} (except prices.csv)."
                )
            load_csv = fallback_candidates[0]
        else:
            load_csv = candidates[0]
    prices_csv = str(P.INPUT_PRICES_CSV) if P.INPUT_PRICES_CSV.is_file() else ""
    out = piece.piece_function(
        InputModel(load_csv=str(load_csv), prices_csv=prices_csv, scenario_yaml=str(P.INPUT_SCENARIO_YAML))
    )
    return out.load_csv, out.scenario_yaml


def step_catalog_sync(scenario_yaml: str) -> tuple[str, str, str, str]:
    from pieces.CatalogSyncPiece.models import InputModel
    from pieces.CatalogSyncPiece.piece import CatalogSyncPiece

    P.OUT_CATALOG_SYNC.mkdir(parents=True, exist_ok=True)
    piece = CatalogSyncPiece()
    piece.results_path = str(P.OUT_CATALOG_SYNC)
    out = piece.piece_function(InputModel(scenario_yaml=scenario_yaml))
    if out.url_outage_detected:
        print(f"[WARN] CatalogSync URL outage fallback active. See: {out.catalog_manifest_json}")
    return out.pv_catalog_json, out.inverter_catalog_json, out.battery_catalog_json, out.catalog_manifest_json


def step_catalog_ranker(scenario_yaml: str, pv_catalog_json: str) -> str:
    from pieces.CatalogRankerPiece.models import InputModel
    from pieces.CatalogRankerPiece.piece import CatalogRankerPiece

    P.OUT_CATALOG_RANKER.mkdir(parents=True, exist_ok=True)
    piece = CatalogRankerPiece()
    piece.results_path = str(P.OUT_CATALOG_RANKER)
    out = piece.piece_function(InputModel(scenario_yaml=scenario_yaml, pv_catalog_json=pv_catalog_json))
    return out.catalog_ranked_recommendation_json


def step_technical_limits(load_csv: str, scenario_yaml: str) -> tuple[str, str]:
    from pieces.TechnicalLimitsPiece.models import InputModel
    from pieces.TechnicalLimitsPiece.piece import TechnicalLimitsPiece

    P.OUT_TECHNICAL_LIMITS.mkdir(parents=True, exist_ok=True)
    piece = TechnicalLimitsPiece()
    piece.results_path = str(P.OUT_TECHNICAL_LIMITS)
    out = piece.piece_function(InputModel(load_csv=load_csv, scenario_yaml=scenario_yaml))
    return out.technical_limits_json, out.scenario_yaml


def step_sizing_optimization(load_csv: str, scenario_yaml: str, technical_limits_json: str) -> str:
    from pieces.SizingOptimizationPiece.models import InputModel
    from pieces.SizingOptimizationPiece.piece import SizingOptimizationPiece

    P.OUT_SIZING_OPT.mkdir(parents=True, exist_ok=True)
    piece = SizingOptimizationPiece()
    piece.results_path = str(P.OUT_SIZING_OPT)
    out = piece.piece_function(
        InputModel(load_csv=load_csv, scenario_yaml=scenario_yaml, technical_limits_json=technical_limits_json)
    )
    return out.sized_scenario_yaml


def step_solar(load_csv: str, scenario_yaml: str) -> str:
    from pieces.SolarSimPiece.models import InputModel
    from pieces.SolarSimPiece.piece import SolarSimPiece

    P.OUT_SOLAR.mkdir(parents=True, exist_ok=True)
    piece = SolarSimPiece()
    piece.results_path = str(P.OUT_SOLAR)
    out = piece.piece_function(InputModel(load_csv=load_csv, scenario_yaml=scenario_yaml))
    return out.virtual_solar_csv


def step_battery_strategy(load_csv: str, scenario_yaml: str) -> str:
    from pieces.BatteryStrategyOptimizerPiece.models import InputModel
    from pieces.BatteryStrategyOptimizerPiece.piece import BatteryStrategyOptimizerPiece

    P.OUT_BATTERY_STRATEGY.mkdir(parents=True, exist_ok=True)
    piece = BatteryStrategyOptimizerPiece()
    piece.results_path = str(P.OUT_BATTERY_STRATEGY)
    out = piece.piece_function(InputModel(load_csv=load_csv, scenario_yaml=scenario_yaml))
    return out.battery_strategy_recommendation_json


def step_battery(load_csv: str, scenario_yaml: str, virtual_solar_csv: str) -> str:
    from pieces.BatterySimPiece.models import InputModel
    from pieces.BatterySimPiece.piece import BatterySimPiece

    P.OUT_BATTERY.mkdir(parents=True, exist_ok=True)
    piece = BatterySimPiece()
    piece.results_path = str(P.OUT_BATTERY)
    out = piece.piece_function(
        InputModel(load_csv=load_csv, scenario_yaml=scenario_yaml, virtual_solar_csv=virtual_solar_csv)
    )
    return out.virtual_battery_soc_csv


def step_simulate(
    load_csv: str,
    scenario_yaml: str,
    ranked_catalog_json: str = "",
    inverter_catalog_json: str = "",
    battery_catalog_json: str = "",
    catalog_manifest_json: str = "",
) -> str:
    from pieces.SimulatePiece.models import InputModel
    from pieces.SimulatePiece.piece import SimulatePiece

    P.OUT_SIMULATE.mkdir(parents=True, exist_ok=True)
    piece = SimulatePiece()
    piece.results_path = str(P.OUT_SIMULATE)
    out = piece.piece_function(
        InputModel(
            load_csv=load_csv,
            scenario_yaml=scenario_yaml,
            output_dir="",
            ranked_catalog_json=ranked_catalog_json,
            inverter_catalog_json=inverter_catalog_json,
            battery_catalog_json=battery_catalog_json,
            catalog_manifest_json=catalog_manifest_json,
        )
    )
    return out.report_json


def step_kpi(report_json: str) -> str:
    from pieces.KPIPiece.models import InputModel
    from pieces.KPIPiece.piece import KPIPiece

    P.OUT_KPI.mkdir(parents=True, exist_ok=True)
    piece = KPIPiece()
    piece.results_path = str(P.OUT_KPI)
    out = piece.piece_function(InputModel(report_json=report_json))
    return out.kpi_results_csv


def step_investment_eval(report_json: str, kpi_csv: str) -> str:
    from pieces.InvestmentEvalPiece.models import InputModel
    from pieces.InvestmentEvalPiece.piece import InvestmentEvalPiece

    P.OUT_INVESTMENT_EVAL.mkdir(parents=True, exist_ok=True)
    piece = InvestmentEvalPiece()
    piece.results_path = str(P.OUT_INVESTMENT_EVAL)
    out = piece.piece_function(InputModel(report_json=report_json, kpi_results_csv=kpi_csv))
    return out.investment_evaluation_csv


def step_dashboard(report_json: str, kpi_csv: str, inv_csv: str) -> str:
    from pieces.DashboardPiece.models import InputModel
    from pieces.DashboardPiece.piece import DashboardPiece

    P.OUT_DASHBOARD.mkdir(parents=True, exist_ok=True)
    piece = DashboardPiece()
    piece.results_path = str(P.OUT_DASHBOARD)
    out = piece.piece_function(
        InputModel(report_json=report_json, kpi_results_csv=kpi_csv, investment_evaluation_csv=inv_csv)
    )
    return out.dashboard_data_json
