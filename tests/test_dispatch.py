from __future__ import annotations

import numpy as np

from pieces.SimulatePiece.piece import dispatch_battery


def test_dispatch_grid_non_negative_and_soc_bounded():
    n = 48
    dt_h = 0.25
    net = np.linspace(200.0, 400.0, n)
    price = np.linspace(0.08, 0.35, n)
    grid, soc, _p, _exp = dispatch_battery(
        net,
        price,
        dt_h,
        energy_kwh=500.0,
        max_c_rate=0.5,
        eta_c=0.95,
        eta_d=0.95,
        initial_soc_pct=50.0,
        mrk_contract_kw=350.0,
    )
    assert np.all(grid >= -1e-6)
    assert np.all(soc >= -1e-6)
    assert np.all(soc <= 100.0 + 1e-6)


def test_grid_charge_respects_mrk_headroom():
    dt_h = 0.25
    net = np.array([100.0])
    price = np.array([0.05])
    grid, _, _, _ = dispatch_battery(
        net,
        price,
        dt_h,
        energy_kwh=1000.0,
        max_c_rate=1.0,
        eta_c=0.95,
        eta_d=0.95,
        initial_soc_pct=10.0,
        mrk_contract_kw=300.0,
    )
    assert float(grid[0]) <= 300.0 + 1e-3
