# MRK / PV / bateria - naming aligned with original workflow

Projekt je premenovany na naming styl povodneho `pitonak` workflow.

## Pieces

- `UserInputPiece`
- `CatalogSyncPiece`
- `TechnicalLimitsPiece`
- `SizingOptimizationPiece`
- `CatalogRankerPiece`
- `SolarSimPiece`
- `BatteryStrategyOptimizerPiece`
- `BatterySimPiece`
- `SimulatePiece`
- `KPIPiece`
- `InvestmentEvalPiece`
- `DashboardPiece`

Vsetky su pod `pieces/<Name>/` a kazdy ma **iba** `models.py`, `piece.py`, `metadata.json`.

Cela MRK / PV / batéria simulácia je v **`pieces/SimulatePiece/piece.py`** (ziadny `CommonPiece` ani extra moduly mimo pieces).

## Workflow

Pipeline je:

`UserInputPiece -> CatalogSyncPiece -> TechnicalLimitsPiece -> SizingOptimizationPiece -> CatalogRankerPiece -> SolarSimPiece -> BatteryStrategyOptimizerPiece -> BatterySimPiece -> SimulatePiece -> KPIPiece -> InvestmentEvalPiece -> DashboardPiece`

Spustenie:

```text
python run_workflow.py
```

Vystupy:

- `tests/SimulatePiece_Outputs/mrk_savings_report.json`
- `tests/KPIPiece_Outputs/kpi_results.csv`
- `tests/InvestmentEvalPiece_Outputs/investment_evaluation.csv`
- `tests/DashboardPiece_Outputs/dashboard_data.json`

## Streamlit dashboard

Po `python run_workflow.py` môžeš otvoriť vizuálny prehľad (KPI, technológia, graf).

**Dôležité:** príkazy musíš spúšťať z priečinka `pitonak_mrk` (nie z `C:\Windows\System32`).

```text
cd <tvoj-klon>\industry_sg_vre_workflow\pitonak_mrk
pip install -r dependencies/requirements.txt
streamlit run streamlit_app.py
```

Alebo z ľubovoľného adresára (PowerShell) – skript sám nastaví správny priečinok:

```text
& "<plna-cesta-k-klonu>\pitonak_mrk\run_streamlit.ps1"
```

Predvolená cesta k JSON je `tests/DashboardPiece_Outputs/dashboard_data.json`; inú cestu zadáš v bočnom paneli alebo cez premennú `STREAMLIT_DASHBOARD_JSON`.

## Domino metadata

- `.domino/dependencies_map.json`
- `.domino/compiled_metadata.json`

uz pouzivaju iba vyssie uvedene piece nazvy.
