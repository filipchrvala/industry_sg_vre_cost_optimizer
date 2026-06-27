# UC3.2 Cost Optimizer — GitLab / Harbor release

Push **only when GitHub CI and local Domino test pass**.

## 1. GitHub (already done)

- Repo: `filipchrvala/industry_sg_vre_cost_optimizer`
- `config.toml` → `REGISTRY_NAME = "filipchrvala"` (GHCR)
- Image: `ghcr.io/filipchrvala/industry_sg_vre_cost_optimizer:0.1.39-group0`

## 2. Prepare GitLab tree locally

```powershell
cd industry_sg_vre_cost_optimizer_sync
python scripts/generate_onedata_customization.py   # creates *.spice.customization
Copy-Item config.spice.toml config.toml -Force     # Harbor registry in config.toml
```

Verify:

- `.gitlab-ci.yml` present
- `config.toml` has `harbor.testbed.spice-platform.eu/partner/uc3`
- `test_cost_optimizer_onedata.spice.customization` has Harbor `source_image`

## 3. Push to SPICE GitLab

```powershell
git add .gitlab-ci.yml config.spice.toml test_cost_optimizer_onedata.spice.customization scripts/
git commit -m "Prepare UC3.2 for SPICE GitLab Harbor CI"
git push spice main
```

GitLab CI (`validate-and-organize`) builds and publishes to Harbor when `config.toml` changes.

## 4. Workflows pack (use-cases/uc3/workflows)

Copy `test_cost_optimizer_onedata.spice.customization` into  
`uc3-domino-gitlab-sync/cost_optimizer_onedata/UC3.2_cost_optimizer_onedata.json`  
and push that repo.

## 5. Domino on SPICE testbed

- Register piece repo from GitLab URL
- Import workflow JSON from workflows pack
- OneData inputs under `FilipsSpace/cost_optimizer/inputs/`
