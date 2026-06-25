# Industry Smart Grid VRE Cost Optimizer – Domino Pieces Repository

**Owner:** filipchrvala  
**Project:** Industry Smart Grid VRE Cost Optimizer

## Structure

- `pieces/`: Domino Pieces
- `dependencies/`: Docker and requirements files
- `config.toml`: Repository configuration
- `.github/workflows/`: CI/CD for building and publishing Pieces

## Usage

1. Install Domino CLI
2. Run: `domino-pieces publish`

## Pieces Overview

| Piece | Purpose |
|-------|---------|
| UserInputPiece | Normalize scenario/load/price inputs for technical and economic pipeline. |
| CatalogSyncPiece | Sync PV and inverter catalogs from online SAM sources. |
| TechnicalLimitsPiece | Compute technical bounds for PV/BESS sizing search space. |
| SizingOptimizationPiece | Run auto/manual sizing for PV and battery. |
| CatalogRankerPiece | Rank hardware catalog options for selected scenario sizing. |
| SolarSimPiece | Generate synthetic PV profile for selected sizing. |
| BatteryStrategyOptimizerPiece | Create battery dispatch strategy recommendation. |
| BatterySimPiece | Generate battery SOC timeseries from load and PV profile. |
| SimulatePiece | Run MRK simulation (baseline/PV/battery) and savings report. |
| KPIPiece | Compute KPI summary from MRK report. |
| InvestmentEvalPiece | Compute investment evaluation from simulated KPIs. |
| DashboardPiece | Aggregate MRK report and KPI CSV into dashboard JSON. |

## Main Outputs

- `tests/SimulatePiece_Outputs/mrk_savings_report.json`
- `tests/KPIPiece_Outputs/kpi_results.csv`
- `tests/InvestmentEvalPiece_Outputs/investment_evaluation.csv`
- `tests/DashboardPiece_Outputs/dashboard_data.json`

## Test Dependencies

- `requirements-tests.txt` is recommended to keep in the repo.
- It is used by test/CI pipelines (for example `pytest`) and keeps test dependencies separated from runtime dependencies.

## OneData workflow (UC3.2)

Static inputs live on OneData under `onedata:///FilipsSpace/cost_optimizer/inputs/`.
Each workflow run writes to its own subfolder: `.../cost_optimizer/outputs/<run_id>/<PieceName>/`.

| File | Purpose |
|------|---------|
| `test_cost_optimizer_onedata.customization` | Domino import — OneData paths + secrets |
| `test_cost_optimizer_local.customization` | Local Domino — `/home/shared_storage/cost_optimizer/inputs/` |

### Local Domino (GitHub image `ghcr.io/filipchrvala/...`)

1. Build/publish image `0.1.34-group0` (or bump `config.toml` VERSION).
2. Seed shared storage inside Domino: `python scripts/seed_shared_storage_cost_optimizer.py`
3. Import `test_cost_optimizer_local.customization` and run.

### OneData testbed

1. `set ONEDATA_TOKEN=...` then `python scripts/seed_onedata_cost_optimizer_inputs.py`
2. Import `test_cost_optimizer_onedata.customization`
3. Set workflow secrets: `onedata_token`, optional `onedata_onezone_host`, `onedata_output_dir`

Regenerate workflow JSON after schema changes:

```powershell
python scripts/refresh_compiled_metadata.py
python scripts/generate_onedata_customization.py
```

Local smoke (no OneData): `python scripts/_local_workflow_smoke.py`

