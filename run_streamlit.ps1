# Spusti Streamlit dashboard z lubovolneho adresara (PowerShell).
$Root = $PSScriptRoot
Set-Location $Root
streamlit run (Join-Path $Root "streamlit_app.py")
