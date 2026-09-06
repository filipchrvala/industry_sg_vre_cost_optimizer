<#
.SYNOPSIS
  Spustí vstupnú webovú stránku a otvorí ju v predvolenom prehliadači.

  powershell -ExecutionPolicy Bypass -File scripts\start_local_web.ps1
#>
param(
    [int]$Port = 8088
)
$ErrorActionPreference = "Stop"
$Root = Resolve-Path (Join-Path $PSScriptRoot "..")
Set-Location $Root

$python = Get-Command python3 -ErrorAction SilentlyContinue
if (-not $python) { $python = Get-Command python -ErrorAction SilentlyContinue }
if (-not $python) { throw "Python nie je v PATH. Nainštalujte Python 3.10+." }

if (-not (Test-Path (Join-Path $Root "examples\demo_site\load_and_prices.csv"))) {
    & $python.Source scripts\make_demo_inputs.py
}

& $python.Source scripts\start_local_web.py --port $Port
