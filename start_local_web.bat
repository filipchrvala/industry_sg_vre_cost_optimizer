@echo off
rem Double-click this file, or run it from any directory.
rem It always starts from the folder that contains this .bat.
cd /d "%~dp0"

where python >nul 2>nul
if errorlevel 1 (
    echo.
    echo Python sa nenasiel. Na Windows pouzite prikaz "python", nie "python3".
    echo Ak Python mate, pridajte ho do PATH, alebo otvorte novy PowerShell.
    echo.
    pause
    exit /b 1
)

echo Projekt: %cd%
echo Spustam http://127.0.0.1:8088/
echo Toto okno nechajte otvorene.
echo.
python scripts\start_local_web.py %*
if errorlevel 1 (
    echo.
    echo Spustenie zlyhalo.
    pause
)
