# UC3.2 Industry SG VRE Cost Optimizer (Domino)

Domino piece repository for the MRK cost-optimizer workflow: historical load in, sized PV and battery out, with an AI production forecast and a CFO dashboard.

## Local run in the browser

Start the input page, fill in the site (or keep the demo values), press **Spustiť výpočet**, watch the 23-piece workflow, then the browser opens the dashboard.

Linux / macOS:

```bash
bash scripts/start_local_web.sh
```

Windows — first `cd` into the project (not `C:\WINDOWS\system32`). On this PC the command is `python`, not `python3`.

Easiest: double-click `start_local_web.bat` in the project folder.

Or in PowerShell:

```powershell
cd C:\Users\NTB\Domino\industry_sg_vre_workflow
powershell -ExecutionPolicy Bypass -File scripts\start_local_web.ps1
```

If the repo lives somewhere else, `cd` there instead. You can find it with:

```powershell
Get-ChildItem -Path C:\Users\NTB -Filter start_local_web.bat -Recurse -ErrorAction SilentlyContinue
```

Same thing as a Python module (from the project folder):

```powershell
python scripts\make_demo_inputs.py
python scripts\start_local_web.py
```

The page is `http://127.0.0.1:8088/`. If that port is taken, pass `--port 8090`. Leave the server running while you use the page.

CSV columns: `datetime`, `load_kw`, `price_eur_per_kwh`. If you do not upload a file, the Nitra demo year is used.

## Local run from the terminal (no browser)

```bash
python3 scripts/make_demo_inputs.py
python3 scripts/run_local.py
```

Outputs land in `.local_run/`. Open `.local_run/DashboardPiece/dashboard.html`.

## Local Domino on this PC

A Cloud Agent on cursor.com cannot reach Docker Desktop on your laptop. On the machine where Domino is already running:

```powershell
git checkout cursor/uc32-production-release-cbe4
powershell -ExecutionPolicy Bypass -File scripts\run_on_local_domino.ps1
```

Then import `test_cost_optimizer_local.customization` in the Domino UI and run it. That file is the same 23-piece graph as production, with `UserInputPiece` pointed at `examples/demo_site/` instead of OneData.

To let a Cloud Agent drive that Domino itself, start a Cursor self-hosted worker on the same PC (`cursor worker start`) and attach the next agent to it. Until then, the agent and the containers live on different machines.

## Production layout

| Path | Purpose |
|------|---------|
| `pieces/` | Domino pieces + `common/` helpers |
| `dependencies/` | Container build (`Dockerfile`, `requirements.txt`) |
| `config.toml` | Version and Harbor registry (GitLab CI) |
| `.domino/` | Compiled metadata (CI) |
| `.gitlab-ci.yml` | Harbor build on GitLab |
| `.github/workflows/` | GHCR build on GitHub (retag from Harbor organize) |
| `webapp/` | Local input form and progress page |
| `scripts/start_local_web.py` | Opens the form in the browser |
| `scripts/build_workflow.py` | Source of truth for the Domino DAG |
| `test_cost_optimizer_onedata.customization` | Domino import (GitHub / GHCR) |
| `test_cost_optimizer_local.customization` | Same graph, demo files on disk (local Domino) |
| `test_cost_optimizer_onedata.spice.customization` | Domino import (SPICE / Harbor) |
| `test_cost_optimizer_onedata.json` | Workflows pack export (GHCR metadata) |

## Domino on SPICE

1. Register GitLab repo as pieces repository (wait for CI `0.1.40-group0` on Harbor).
2. Import `test_cost_optimizer_onedata.spice.customization`.
3. OneData inputs under `onedata:///FilipsSpace/cost_optimizer/inputs/`.
4. Optional secrets: `onedata_token`, `onedata_onezone_host`, `onedata_output_dir`.

## GitLab CI / Harbor

Pipeline runs on push to `main` when `config.toml` changes (same as UC3.3).

Set **Settings → CI/CD → Variables** (mask secrets) — **all five** are required:

| Variable | Description |
|----------|-------------|
| `CI_PUSH_TOKEN` | Project access token (`write_repository` + `api`) |
| `CI_RELEASE_TOKEN` | Same token (`api`) |
| `CONTAINER_REGISTRY` | `harbor.testbed.spice-platform.eu` |
| `CONTAINER_REGISTRY_USERNAME` | e.g. `partner` |
| `CONTAINER_REGISTRY_PASSWORD` | Harbor password (SPICE vault) |

UC3.3 already has these; UC3.2 is a **separate GitLab project** and needs the same setup once.

Automated setup (PowerShell, do not paste secrets into chat):

```powershell
cd C:\Users\NTB\Domino\industry_sg_vre_workflow
$env:GITLAB_TOKEN = "glpat-..."      # Maintainer, api
$env:HARBOR_PASSWORD = "..."         # same as UC3.3
powershell -ExecutionPolicy Bypass -File scripts\setup_uc32_gitlab_ci.ps1 -TriggerPipeline
```

Check variables: `scripts\check_uc33_gitlab_ci.ps1 -ProjectId 91 -ProjectPath use-cases/uc3/UC3.2_Industry_Sg_Vre_Cost_Optimizer`

Delete failed pipeline runs: `scripts\delete_uc32_failed_pipelines.ps1` (needs Maintainer).

## Regenerate workflow exports

```powershell
python scripts/build_workflow.py
python scripts/export_workflow_json.py
```

## Pieces

UserInput → CatalogSync / TechnicalLimits / Open-Meteo → UC3.4 AI chain (+ SHMÚ calibration) → PvoutToVirtualSolar → Sizing → CatalogRanker / BatteryStrategy → BatterySim → Simulate / Heatmap → KPI / InvestmentEval → Dashboard.
