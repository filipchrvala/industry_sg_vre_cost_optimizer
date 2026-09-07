<#
.SYNOPSIS
  Prepare this branch for a run on the Domino that is already running on this PC.

A Cloud Agent cannot see Docker Desktop on your laptop, and this script cannot
see a Cloud Agent VM. Run it on the machine where Domino is actually up.
#>
[CmdletBinding()]
param(
    [string]$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path,
    [string]$DominoUrl = "http://localhost:3000"
)

$ErrorActionPreference = "Stop"

function Write-Step($msg) { Write-Host "`n==> $msg" -ForegroundColor Cyan }

Write-Step "Looking for a running Domino on this machine"
try {
    $containers = docker ps --format "{{.Names}}`t{{.Image}}`t{{.Ports}}`t{{.Status}}"
} catch {
    Write-Host "Docker CLI is not available. Start Docker Desktop first." -ForegroundColor Red
    exit 2
}

$match = $containers | Where-Object { $_ -match "domino|frontend|airflow" }
if ($match) {
    Write-Host "Containers that look like Domino:"
    $match | ForEach-Object { Write-Host "  $_" }
} else {
    Write-Host "No Domino-looking container in 'docker ps'. Full list:" -ForegroundColor Yellow
    $containers | ForEach-Object { Write-Host "  $_" }
}

$ui = $null
foreach ($url in @($DominoUrl, "http://localhost:3000", "http://localhost:8000")) {
    try {
        $resp = Invoke-WebRequest -Uri $url -UseBasicParsing -TimeoutSec 3
        Write-Host "UI responds at $url (HTTP $($resp.StatusCode))" -ForegroundColor Green
        $ui = $url
        break
    } catch { }
}
if (-not $ui) {
    Write-Host "Nothing answered on :3000 or :8000. Open the UI yourself if it uses another port." -ForegroundColor Yellow
    $ui = $DominoUrl
}

Write-Step "Regenerating import files"
Push-Location $RepoRoot
try {
    python scripts/make_demo_inputs.py
    python scripts/build_workflow.py
    if ($LASTEXITCODE -ne 0) { throw "build_workflow.py failed" }
    python scripts/export_workflow_json.py
} finally {
    Pop-Location
}

$localImport = Join-Path $RepoRoot "test_cost_optimizer_local.customization"
Write-Host ""
Write-Host "Ready." -ForegroundColor Green
Write-Host "  1. Open $ui"
Write-Host "  2. Workflows -> Import"
Write-Host "  3. File: $localImport"
Write-Host "  4. Pieces repository = this checkout, branch cursor/uc32-production-release-cbe4"
Write-Host "  5. Run"
Write-Host ""
Write-Host "OneData import (unchanged): $(Join-Path $RepoRoot 'test_cost_optimizer_onedata.customization')"
