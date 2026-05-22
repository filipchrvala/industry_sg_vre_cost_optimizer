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
