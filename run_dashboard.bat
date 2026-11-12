@echo off
setlocal EnableExtensions

cd /d "%~dp0"

if not exist ".venv\Scripts\python.exe" (
  echo [ERROR] Project virtual environment not found: .venv\Scripts\python.exe
  echo Run scripts\setup_dashboard.bat or create the project .venv first.
  exit /b 1
)

set "DASHBOARD_HOST=%DASHBOARD_HOST%"
if "%DASHBOARD_HOST%"=="" set "DASHBOARD_HOST=127.0.0.1"
set "DASHBOARD_PORT=%DASHBOARD_PORT%"
if "%DASHBOARD_PORT%"=="" set "DASHBOARD_PORT=8090"

echo Starting E-commerce Order Lifecycle Streaming dashboard...
echo URL: http://%DASHBOARD_HOST%:%DASHBOARD_PORT%
echo Press Ctrl+C to stop.
echo.

".venv\Scripts\python.exe" -m uvicorn dashboard.backend.app.main:app --host %DASHBOARD_HOST% --port %DASHBOARD_PORT%
set "EXIT_CODE=%ERRORLEVEL%"

if not "%EXIT_CODE%"=="0" (
  echo.
  echo [ERROR] Dashboard exited with code %EXIT_CODE%.
)

exit /b %EXIT_CODE%
