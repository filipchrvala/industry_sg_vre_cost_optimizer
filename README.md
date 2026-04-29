# Industry Smart Grid VRE Cost Optimizer – Domino Pieces Repository

**Owner:** SCDI 
**Project:** Industry Smart Grid VRE Cost Optimizer

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

