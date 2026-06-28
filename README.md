# UC3.2 Industry SG VRE Cost Optimizer (Domino)

Domino piece repository for the MRK cost-optimizer workflow (OneData I/O, PV/battery simulation, KPIs, dashboard).

## Production layout

| Path | Purpose |
|------|---------|
| `pieces/` | Domino pieces + `common/` helpers |
| `dependencies/` | Container build (`Dockerfile`, `requirements.txt`) |
| `config.toml` | Version and Harbor registry (GitLab CI) |
| `.domino/` | Compiled metadata (CI) |
| `.gitlab-ci.yml` | Harbor build on GitLab |
| `.github/workflows/` | GHCR build on GitHub (retag from Harbor organize) |
| `Test.customization` | Workflow graph source (regenerate exports from this) |
| `test_cost_optimizer_onedata.customization` | Domino import (GitHub / GHCR) |
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
python scripts/sync_test_customization.py
python scripts/generate_onedata_customization.py
python scripts/export_workflow_json.py
```

## Pieces

UserInput → CatalogSync → TechnicalLimits → Sizing → CatalogRanker → SolarSim → BatteryStrategy → BatterySim → Simulate → KPI / InvestmentEval → Dashboard.
