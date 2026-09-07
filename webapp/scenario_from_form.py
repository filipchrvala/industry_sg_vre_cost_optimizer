"""Turn the web form into the scenario.yaml the pieces already read."""

from __future__ import annotations

from typing import Any


def _f(data: dict[str, Any], key: str, default: float) -> float:
    raw = data.get(key, default)
    try:
        return float(raw)
    except (TypeError, ValueError):
        return float(default)


def _i(data: dict[str, Any], key: str, default: int) -> int:
    return int(_f(data, key, default))


def _b(data: dict[str, Any], key: str, default: bool = False) -> bool:
    raw = data.get(key, default)
    if isinstance(raw, bool):
        return raw
    return str(raw).strip().lower() in {"1", "true", "on", "yes", "ano", "áno"}


def build_scenario(data: dict[str, Any]) -> dict[str, Any]:
    scope = str(data.get("system_scope") or "pv_and_battery")
    mode = str(data.get("selection_mode") or "auto")
    require_battery = _b(data, "require_battery", scope == "pv_and_battery")
    return {
        "site": {
            "name": str(data.get("site_name") or "Priemyselný odberateľ").strip(),
            "latitude": _f(data, "latitude", 48.3061),
            "longitude": _f(data, "longitude", 18.0764),
            "altitude_m": _f(data, "altitude_m", 144),
            "timezone": str(data.get("timezone") or "Europe/Bratislava"),
        },
        "use_pv": scope not in ("battery_only", "battery"),
        "use_battery": scope not in ("pv_only", "pv"),
        "pv": {
            "installed_kwp": _f(data, "installed_kwp", 600.0),
            "panel_tilt": _f(data, "panel_tilt", 25.0),
            "azimuth": _f(data, "azimuth", 180.0),
            "yield_kwh_per_kwp_year": _f(data, "yield_kwh_per_kwp_year", 1050.0),
            "specific_capex_eur_per_kwp": _f(data, "specific_capex_eur_per_kwp", 720.0),
            "om_eur_per_kwp_year": _f(data, "om_eur_per_kwp_year", 11.0),
            "degradation_pct_per_year": _f(data, "degradation_pct_per_year", 0.45),
            "mount_type": str(data.get("mount_type") or "roof"),
        },
        "battery": {
            "energy_kwh": _f(data, "energy_kwh", 800.0),
            "max_c_rate": _f(data, "max_c_rate", 0.5),
            "charge_efficiency": _f(data, "charge_efficiency", 0.96),
            "discharge_efficiency": _f(data, "discharge_efficiency", 0.96),
            "initial_soc_pct": 50.0,
            "specific_capex_eur_per_kwh": _f(data, "specific_capex_eur_per_kwh", 295.0),
            "cycle_life": _i(data, "cycle_life", 6000),
            "max_fraction_capacity_from_grid_charge": 0.72,
            "peak_shaving_reserve_pct": 30.0,
        },
        "mrk": {
            "contract_kw": _f(data, "contract_kw", 1000.0),
            "fee_eur_per_kw_month": _f(data, "fee_eur_per_kw_month", 4.2),
            "excess_peak_penalty_eur_per_kw": _f(data, "excess_peak_penalty_eur_per_kw", 12.0),
        },
        "energy": {
            "feed_in_surplus_eur_per_kwh": _f(data, "feed_in_surplus_eur_per_kwh", 0.045),
            "distribution_eur_per_kwh": _f(data, "distribution_eur_per_kwh", 0.041),
        },
        "analysis": {
            "amortization_years": _i(data, "amortization_years", 15),
            "discount_rate": _f(data, "discount_rate", 0.075),
            "enable_trading_only_scenario": True,
        },
        "equipment": {
            "selection_mode": mode,
            "system_scope": scope,
            "auto": {
                "objective": str(data.get("objective") or "max_npv"),
                "kwp_step": _f(data, "kwp_step", 100.0),
                "kwh_step": _f(data, "kwh_step", 200.0),
                "min_pv_kwp": _f(data, "min_pv_kwp", 100.0),
                "min_battery_kwh": (
                    _f(data, "min_battery_kwh", 200.0) if require_battery else 0.0
                ),
                "require_battery": require_battery,
                "max_configurations": 120,
            },
            "constraints": {
                "max_roof_area_m2": _f(data, "max_roof_area_m2", 7800.0),
                "max_battery_area_m2": _f(data, "max_battery_area_m2", 160.0),
                "max_capex_eur": _f(data, "max_capex_eur", 1_400_000.0),
                "roof_load_limit_kg_per_m2": _f(data, "roof_load_limit_kg_per_m2", 25.0),
                "installation": {
                    "mount_type": str(data.get("mount_type") or "roof"),
                    "shading": str(data.get("shading") or "low"),
                    "priority": str(data.get("priority") or "balanced"),
                    "allow_bifacial": True,
                },
            },
            "layout": {
                "kwp_per_m2_roof": 0.19,
                "kwh_per_m2_battery_area": 30.0,
            },
        },
    }
