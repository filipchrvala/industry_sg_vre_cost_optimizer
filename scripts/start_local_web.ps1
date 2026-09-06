<#
.SYNOPSIS
  Spustí vstupnú webovú stránku a otvorí ju v predvolenom prehliadači.

  Z priečinka projektu:
    powershell -ExecutionPolicy Bypass -File scripts\start_local_web.ps1

  Z ľubovoľného priečinka (cestu upravte):
    powershell -ExecutionPolicy Bypass -File C:\cesta\k\projektu\scripts\start_local_web.ps1

  Alebo dvojklik na start_local_web.bat v koreni projektu.
#>
param(
    [int]$Port = 8088
)
$ErrorActionPreference = "Stop"
$Root = Resolve-Path (Join-Path $PSScriptRoot "..")
Set-Location $Root

function Find-Python {
    # On Windows, `python3` is often the Store stub ("Python sa nenašiel").
    $names = if ($env:OS -like "*Windows*") { @("python", "py", "python3") } else { @("python3", "python") }
    foreach ($name in $names) {
        $cmd = Get-Command $name -ErrorAction SilentlyContinue
        if (-not $cmd) { continue }
        if ($cmd.Source -like "*WindowsApps*") { continue }
        return $cmd
    }
    return $null
}

$python = Find-Python
if (-not $python) {
    throw "Python nie je v PATH. Na Windows píšte 'python', nie 'python3'. Nainštalujte Python 3.10+."
}

Write-Host "Projekt: $Root"
Write-Host "Python:  $($python.Source)"

if (-not (Test-Path (Join-Path $Root "examples\demo_site\load_and_prices.csv"))) {
    & $python.Source scripts\make_demo_inputs.py
}

& $python.Source scripts\start_local_web.py --port $Port
