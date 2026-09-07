# UC3.2 Industry SG VRE Cost Optimizer

Domino pieces repository. Register `filipchrvala/industry_sg_vre_cost_optimizer` at version `0.1.44`, then import `UC3.2.customization`.

Inputs (OneData):

- `onedata:///SCDI/UC3.2_COST_OPTIMIZER/inputs/load_and_prices.csv` — load is required; the price column is optional
- `onedata:///SCDI/UC3.2_COST_OPTIMIZER/inputs/scenario.yaml`
- `prices_csv` in the import is empty on purpose

Sample files for those paths are in `examples/demo_site/`.

Prices are optional. If `load_csv` has no usable `price_eur_per_kwh` and `prices_csv` is empty, UserInputPiece pulls OKTE day-ahead prices for the same datetime range as the load (hourly before 2025-10-01, 15-minute afterwards; hourly values are repeated onto 15-min load steps). Uploaded prices always win over OKTE.
