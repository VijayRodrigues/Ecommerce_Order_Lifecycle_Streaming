@echo off
setlocal EnableExtensions

cd /d "%~dp0"

if "%DASHBOARD_HOST%"=="" set "DASHBOARD_HOST=127.0.0.1"
if "%DASHBOARD_PORT%"=="" set "DASHBOARD_PORT=8090"

powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\start_demo.ps1" -HostName "%DASHBOARD_HOST%" -Port "%DASHBOARD_PORT%"
set "EXIT_CODE=%ERRORLEVEL%"

if not "%EXIT_CODE%"=="0" (
  echo.
  echo Launcher failed with exit code %EXIT_CODE%.
)

exit /b %EXIT_CODE%
