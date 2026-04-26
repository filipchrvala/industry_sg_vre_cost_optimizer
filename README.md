# Industry Smart Grid Virtual RE Pilot Project – Domino Pieces Repository

**Owner:** scditech (SCDI)  
**Project:** Industry Smart Grid Virtual RE  

**Domino / vývoj:** kompletný samostatný projekt je v **[`Alternate/`](Alternate/README.md)** (všetky piece, testy, `run_workflow.py`, investičný návrh). Koreňový priečinok môže zostať ako zrkadlo alebo história.

## Structure

- `pieces/`: Domino Pieces
- `dependencies/`: Docker and requirements files
- `config.toml`: Repository configuration
- `.github/workflows/`: CI/CD for building Pieces

## Lokálny pipeline (bez `workflow_new`)

Z koreňa tohto repa:

```bash
python run_workflow.py
```

Preprocess ukladá len `train_dataset.parquet`; predikcia ide zo samostatného CSV (`scripts/generate_predict_planned_csv.py` ak treba). Žiadny extra priečinok `workflow_new` — všetko je v `pieces/`, `tests/`, `scripts/`.

## Usage (Domino publish)

1. Install Domino CLI
2. Run: `domino-pieces publish`

## Pieces Overview

| Piece | Purpose |
|-------|---------|
| FetchEnergyDataPiece | Merge load, production, and price CSVs into one Parquet dataset. |
| PreprocessEnergyDataPiece | Iba `train_dataset.parquet` (15 min); predikčný vstup je samostatný CSV. |
| TrainModelPiece | Tréning XGBoost; v logu MAE/RMSE v kW a % priemeru testu, MAPE. |
| PredictPiece | Predpoveď `predictions_15min.csv`; voliteľne rolling na plánovanom CSV. |
| SolarSimPiece | Simulate PV output (virtual_solar.csv) from weather and solar_config.yml. |
| BatterySimPiece | Simulate battery charge/discharge and grid import (virtual_battery_soc.csv, battery_summary.csv). |
| SimulatePiece | Compute baseline vs. scenario costs (simulated_results.csv, summary.csv). |
| KPIPiece | Compute KPIs: kWh/ton, peak reduction, savings, CO₂ (kpi_results.csv). |
| InvestmentEvalPiece | Investment evaluation: CAPEX, payback, NPV, LCOE (investment_evaluation.csv). |
| DashboardPiece | Aggregate piece outputs into dashboard_data.json for the Streamlit dashboard. |
