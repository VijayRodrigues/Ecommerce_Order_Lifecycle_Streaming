@echo off
setlocal EnableExtensions

cd /d "%~dp0.."

if not exist ".venv\Scripts\python.exe" (
  echo [ERROR] Project virtual environment not found: .venv\Scripts\python.exe
  exit /b 1
)

echo Installing dashboard backend dependencies into the existing project .venv...
".venv\Scripts\python.exe" -m pip install -r dashboard\backend\requirements.txt
if errorlevel 1 (
  echo [ERROR] Dependency installation failed.
  exit /b 1
)

echo.
echo Dashboard dependencies installed.
echo Start the dashboard with: run_dashboard.bat
exit /b 0
