from __future__ import annotations

import json
from pathlib import Path

import yaml
from domino.base_piece import BasePiece

from pieces.SimulatePiece.piece import rank_pv_modules_for_site

from .models import InputModel, OutputModel


class CatalogRankerPiece(BasePiece):
    """Produce top ranked online PV modules for current scenario."""

    def piece_function(self, input_data: InputModel) -> OutputModel:
        scenario_path = Path(input_data.scenario_yaml)
        pv_path = Path(input_data.pv_catalog_json)
        if not scenario_path.is_file():
            raise FileNotFoundError(f"Scenario YAML not found: {scenario_path}")
        if not pv_path.is_file():
            raise FileNotFoundError(f"PV catalog JSON not found: {pv_path}")

        cfg = yaml.safe_load(scenario_path.read_text(encoding="utf-8")) or {}
        inst = ((cfg.get("equipment") or {}).get("constraints") or {}).get("installation") or {}
        installed_kwp = float((cfg.get("pv") or {}).get("installed_kwp", 0.0))
        items = (json.loads(pv_path.read_text(encoding="utf-8")) or {}).get("items") or []
        ranked = rank_pv_modules_for_site(items, installation=inst)
        top = []
        for r in ranked[:10]:
            m = r["module"]
            wp = float(m.get("stc_watts", 0) or 0)
            n_mod = int((installed_kwp * 1000.0 + wp - 1) // max(wp, 1.0)) if wp > 0 else 0
            top.append(
                {
                    "manufacturer": m.get("manufacturer"),
                    "model": m.get("model"),
                    "power_wp": wp,
                    "score": r["score"],
                    "module_count_estimate": n_mod,
                }
            )

        out_dir = Path(self.results_path or scenario_path.parent)
        out_dir.mkdir(parents=True, exist_ok=True)
        out_json = out_dir / "catalog_ranked_recommendation.json"
        out_json.write_text(
            json.dumps(
                {"installed_kwp_target": installed_kwp, "top_recommendations": top},
                indent=2,
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        return OutputModel(message="Catalog ranking finished", catalog_ranked_recommendation_json=str(out_json))
