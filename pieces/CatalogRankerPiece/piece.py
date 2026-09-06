from __future__ import annotations

import json
from pathlib import Path
import traceback

import yaml
from domino.base_piece import BasePiece

try:
    from pieces.simulate_import import load_simulate_module
except ModuleNotFoundError:
    from simulate_import import load_simulate_module

from .models import InputModel, OutputModel

try:
    from common import onedata_io as od
except ModuleNotFoundError:
    try:
        from pieces.common import onedata_io as od
    except ModuleNotFoundError:
        od = None


class CatalogRankerPiece(BasePiece):
    """Produce top ranked online PV modules for current scenario."""

    def piece_function(self, input_data: InputModel, secrets_data=None) -> OutputModel:
        _stage = None
        _piece_out = None
        _run_id = None
        if od is not None:
            input_data, _stage = od.stage_inputs(input_data, secrets_data)
            _run_id = od.resolve_run_id(input_data, secrets_data, generate=False)
        scenario_path = Path(input_data.scenario_yaml)
        pv_path = Path(input_data.pv_catalog_json)
        out_dir = Path(self.results_path or scenario_path.parent)
        out_dir.mkdir(parents=True, exist_ok=True)
        log_path = out_dir / "catalog_ranker.log"

        def _log(msg: str) -> None:
            text = f"[CatalogRankerPiece] {msg}"
            print(text, flush=True)
            with log_path.open("a", encoding="utf-8") as f:
                f.write(text + "\n")

        _log(f"Input scenario_yaml={scenario_path}")
        _log(f"Input pv_catalog_json={pv_path}")
        if not scenario_path.is_file():
            raise FileNotFoundError(f"Scenario YAML not found: {scenario_path}")
        if not pv_path.is_file():
            raise FileNotFoundError(f"PV catalog JSON not found: {pv_path}")

        try:
            sim = load_simulate_module()
            cfg = yaml.safe_load(scenario_path.read_text(encoding="utf-8")) or {}
            inst = ((cfg.get("equipment") or {}).get("constraints") or {}).get("installation") or {}
            installed_kwp = float((cfg.get("pv") or {}).get("installed_kwp", 0.0))
            items = (json.loads(pv_path.read_text(encoding="utf-8")) or {}).get("items") or []
            ranked = sim.rank_pv_modules_for_site(items, installation=inst)
            top = []
            for r in ranked[:10]:
                m = r["module"]
                # CatalogSyncPiece now emits power_wp; stc_watts is the legacy
                # name kept for catalogs produced by an older image.
                wp = float(m.get("power_wp") or m.get("stc_watts") or 0)
                n_mod = int((installed_kwp * 1000.0 + wp - 1) // max(wp, 1.0)) if wp > 0 else 0
                area_m2 = m.get("area_m2")
                eur_per_wp = m.get("eur_per_wp")
                top.append(
                    {
                        "manufacturer": m.get("manufacturer"),
                        "model": m.get("model"),
                        "power_wp": wp,
                        "efficiency_pct": m.get("efficiency_pct"),
                        "area_m2": area_m2,
                        "bifacial": m.get("bifacial"),
                        "half_cut": m.get("half_cut"),
                        "eur_per_wp": eur_per_wp,
                        "price_source": m.get("price_source") or m.get("source"),
                        "score": r["score"],
                        "module_count_estimate": n_mod,
                        "array_area_m2_estimate": (
                            round(n_mod * float(area_m2), 1) if area_m2 and n_mod else None
                        ),
                        "module_capex_eur_estimate": (
                            round(installed_kwp * 1000.0 * float(eur_per_wp), 0)
                            if eur_per_wp and installed_kwp > 0
                            else None
                        ),
                    }
                )

            scored_fields = sum(
                1 for m in (r["module"] for r in ranked[:10])
                if m.get("power_wp") and m.get("area_m2") and m.get("efficiency_pct")
            )
            _log(
                f"Ranked {len(items)} modules, top_count={len(top)}, "
                f"{scored_fields}/{len(top)} scored on complete attributes"
            )
        except Exception as exc:
            (out_dir / "catalog_ranker_error.txt").write_text(traceback.format_exc(), encoding="utf-8")
            _log(f"ERROR during catalog ranking: {exc}")
            if od is not None:
                od.cleanup_on_error(self.results_path, secrets_data, "CatalogRankerPiece", _stage, run_id=_run_id)
            raise

        out_json = out_dir / "catalog_ranked_recommendation.json"
        out_json.write_text(
            json.dumps(
                {"installed_kwp_target": installed_kwp, "top_recommendations": top},
                indent=2,
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        _log(f"Wrote output: {out_json}")
        _piece_out = OutputModel(message="Catalog ranking finished", catalog_ranked_recommendation_json=str(out_json))
        if od is not None and _piece_out is not None:
            return od.finish_piece(
                _piece_out, self.results_path, secrets_data, "CatalogRankerPiece", _stage, run_id=_run_id
            )
        if _stage is not None:
            _stage.cleanup()
        return _piece_out
