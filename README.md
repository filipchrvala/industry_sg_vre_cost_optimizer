# UC3.2 Industry SG VRE Cost Optimizer

Domino pieces repository. Register `filipchrvala/industry_sg_vre_cost_optimizer` at version `0.1.45`, then import `UC3.2.customization`.

Upload these files to OneData **before the first run** (create the `inputs` folder if it is missing):

- `onedata:///SCDI/UC3.2_COST_OPTIMIZER/inputs/load_and_prices.csv`
- `onedata:///SCDI/UC3.2_COST_OPTIMIZER/inputs/scenario.yaml`

Samples are in `examples/demo_site/`. The load file may be named `load.csv` locally, but the import looks for **`load_and_prices.csv`**. `prices_csv` stays empty.

Prices are optional. If `load_csv` has no usable `price_eur_per_kwh` and `prices_csv` is empty, UserInputPiece pulls OKTE day-ahead prices for the same datetime range as the load.

## Lokálne cez web (bez Domina)

Celý výpočet ide spustiť na tomto počítači. Otvorí sa vstupná stránka, po dokončení dashboard.

V PowerShell, z koreňa repozitára:

```powershell
python -m pip install -r dependencies/requirements_0.txt
powershell -ExecutionPolicy Bypass -File scripts\start_local_web.ps1
```

Alebo dvojklik na `scripts\start_local_web.cmd`.

Stránka beží na [http://127.0.0.1:8088/](http://127.0.0.1:8088/). Nahrajte odber (CSV alebo Excel). Ceny sú voliteľné — ak ich súbor nemá, stiahne sa OKTE ISOT DAM. Iný port: `python scripts\start_local_web.py --port 8090`.

Bez prehliadača, rovnaký výpočet z príkazového riadka:

```powershell
python scripts\make_demo_inputs.py
python scripts\run_local.py --inputs examples\demo_site --out .local_run
```

Dashboard je potom v `.local_run\DashboardPiece\dashboard.html`.
