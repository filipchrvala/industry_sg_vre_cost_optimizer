"""Run the whole workflow locally, outside Domino.

The DAG comes from scripts/build_workflow.py, the same declaration the Domino
import file is generated from, so a local run exercises the wiring that will
actually be deployed. If a piece renames an output, this run fails in the same
place the platform would.

    python3 scripts/make_demo_inputs.py
    python3 scripts/run_local.py --inputs examples/demo_site --out .local_run

`domino.base_piece` is stubbed with the harness in pieces/local_compat, so no
Domino installation is needed.
"""

from __future__ import annotations

import argparse
import importlib
import json
import sys
import time
import traceback
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
PIECES = ROOT / "pieces"


def install_domino_stub() -> None:
    """Point `domino.base_piece` at the local harness before any piece imports."""
    sys.path.insert(0, str(ROOT))
    sys.path.insert(0, str(PIECES))

    from local_compat import install_domino_stub as install

    install()


def topological_order(workflow: dict[str, dict]) -> list[str]:
    incoming = {name: set() for name in workflow}
    for name, spec in workflow.items():
        for value in (spec.get("inputs") or {}).values():
            if isinstance(value, tuple):
                incoming[name].add(value[0])

    ordered: list[str] = []
    remaining = dict(incoming)
    while remaining:
        ready = sorted(n for n, deps in remaining.items() if not (deps - set(ordered)))
        if not ready:
            raise RuntimeError(f"Cycle among: {sorted(remaining)}")
        for name in ready:
            ordered.append(name)
            remaining.pop(name)
    return ordered


def coerce(value: Any) -> Any:
    """Pydantic models are not JSON, and outputs are passed between pieces as data."""
    if hasattr(value, "model_dump"):
        return value.model_dump()
    if isinstance(value, list):
        return [coerce(v) for v in value]
    return value


def run_piece(
    name: str,
    spec: dict,
    outputs: dict[str, dict],
    out_root: Path,
    overrides: dict[str, dict],
) -> tuple[dict | None, str]:
    piece_module = importlib.import_module(f"{name}.piece")
    models_module = importlib.import_module(f"{name}.models")
    piece_class = getattr(piece_module, name)
    input_model = models_module.InputModel

    kwargs: dict[str, Any] = {}
    for field, value in (spec.get("inputs") or {}).items():
        if isinstance(value, tuple):
            src_piece, src_arg = value
            upstream = outputs.get(src_piece)
            if upstream is None:
                return None, f"upstream {src_piece} produced nothing"
            if src_arg not in upstream:
                return None, f"{src_piece} has no output '{src_arg}'"
            kwargs[field] = coerce(upstream[src_arg])
        else:
            kwargs[field] = value
    kwargs.update(overrides.get(name, {}))

    results_path = out_root / name
    results_path.mkdir(parents=True, exist_ok=True)

    piece = piece_class(results_path=str(results_path))

    # Pieces ported from UC3.4 declare piece_function(self, input_data) without
    # the secrets argument, so it cannot be passed unconditionally.
    import inspect

    signature = inspect.signature(piece.piece_function)
    if len(signature.parameters) >= 2:
        result = piece.piece_function(input_model(**kwargs), None)
    else:
        result = piece.piece_function(input_model(**kwargs))
    return coerce(result), ""


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inputs", type=Path, default=ROOT / "examples" / "demo_site")
    parser.add_argument("--out", type=Path, default=ROOT / ".local_run")
    parser.add_argument(
        "--skip",
        nargs="*",
        default=[],
        help="Pieces to skip, for example ShmuCalibrationPiece when offline.",
    )
    parser.add_argument(
        "--stop-on-error",
        action="store_true",
        help="Stop at the first failure instead of reporting every piece.",
    )
    args = parser.parse_args()

    install_domino_stub()
    sys.path.insert(0, str(ROOT / "scripts"))
    from build_workflow import WORKFLOW  # noqa: E402

    load_csv = (args.inputs / "load_and_prices.csv").resolve()
    scenario_yaml = (args.inputs / "scenario.yaml").resolve()
    for path in (load_csv, scenario_yaml):
        if not path.is_file():
            print(f"Missing input: {path}\nRun scripts/make_demo_inputs.py first.")
            return 2

    overrides = {
        "UserInputPiece": {
            "load_csv": str(load_csv),
            "prices_csv": "",
            "scenario_yaml": str(scenario_yaml),
        }
    }

    args.out.mkdir(parents=True, exist_ok=True)
    outputs: dict[str, dict] = {}
    report: list[dict] = []
    skipped = set(args.skip)

    for name in topological_order(WORKFLOW):
        if name in skipped:
            print(f"SKIP  {name}")
            report.append({"piece": name, "status": "skipped"})
            continue

        started = time.time()
        try:
            result, problem = run_piece(name, WORKFLOW[name], outputs, args.out, overrides)
        except Exception as exc:  # noqa: BLE001
            elapsed = time.time() - started
            detail = traceback.format_exc()
            (args.out / f"{name}.error.txt").write_text(detail, encoding="utf-8")
            print(f"FAIL  {name} ({elapsed:.1f}s): {type(exc).__name__}: {exc}")
            report.append(
                {"piece": name, "status": "failed", "seconds": round(elapsed, 2), "error": str(exc)}
            )
            if args.stop_on_error:
                break
            continue

        elapsed = time.time() - started
        if result is None:
            print(f"FAIL  {name} ({elapsed:.1f}s): {problem}")
            report.append(
                {"piece": name, "status": "failed", "seconds": round(elapsed, 2), "error": problem}
            )
            if args.stop_on_error:
                break
            continue

        outputs[name] = result
        message = str(result.get("message", "")).strip()
        print(f"OK    {name} ({elapsed:.1f}s){f': {message}' if message else ''}")
        report.append({"piece": name, "status": "ok", "seconds": round(elapsed, 2)})

    summary = args.out / "run_summary.json"
    summary.write_text(
        json.dumps({"pieces": report, "outputs": outputs}, indent=2, ensure_ascii=False, default=str),
        encoding="utf-8",
    )

    failed = [r["piece"] for r in report if r["status"] == "failed"]
    print(f"\n{len(report) - len(failed) - len(skipped)} ok, {len(failed)} failed, {len(skipped)} skipped")
    print(f"Summary: {summary}")
    if failed:
        print("Failed: " + ", ".join(failed))
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
