param(
    [string]$HostName = $env:DASHBOARD_HOST,
    [string]$Port = $env:DASHBOARD_PORT
)

$ErrorActionPreference = "Stop"
if ([string]::IsNullOrWhiteSpace($HostName)) { $HostName = "127.0.0.1" }
if ([string]::IsNullOrWhiteSpace($Port)) { $Port = "8090" }

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Set-Location $repoRoot

$python = Join-Path $repoRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $python)) {
    Write-Host "[ERROR] Project virtual environment not found: $python" -ForegroundColor Red
    Write-Host "Run scripts\setup_dashboard.bat or create the project .venv first."
    exit 1
}

$url = "http://$HostName`:$Port"
$healthUrl = "$url/api/health"

function Test-Health {
    try {
        $resp = Invoke-WebRequest -Uri $healthUrl -UseBasicParsing -TimeoutSec 2
        return ($resp.StatusCode -eq 200)
    } catch {
        return $false
    }
}

function Test-PortInUse {
    try {
        $conn = Get-NetTCPConnection -LocalPort [int]$Port -State Listen -ErrorAction SilentlyContinue | Select-Object -First 1
        return $null -ne $conn
    } catch {
        return $false
    }
}

Write-Host "E-commerce Order Lifecycle Streaming - One-Click Demo Launcher" -ForegroundColor Cyan
Write-Host "Repository: $repoRoot"
Write-Host "Dashboard URL: $url"
Write-Host ""

if (Test-Health) {
    Write-Host "[OK] Dashboard backend is already running and healthy." -ForegroundColor Green
    Write-Host "Opening browser..."
    Start-Process $url | Out-Null
    Write-Host "Use the 'Start Demo Environment' button in the dashboard to prepare infrastructure."
    exit 0
}

if (Test-PortInUse) {
    Write-Host "[ERROR] Port $Port is already in use but the dashboard health endpoint is not responding." -ForegroundColor Red
    Write-Host "Stop the conflicting process or set DASHBOARD_PORT to a free port, then retry."
    exit 1
}

Write-Host "Starting dashboard backend..."
$argList = @("-m", "uvicorn", "dashboard.backend.app.main:app", "--host", $HostName, "--port", $Port)
$proc = Start-Process -FilePath $python -ArgumentList $argList -WorkingDirectory $repoRoot -NoNewWindow -PassThru

$deadline = (Get-Date).AddSeconds(45)
$healthy = $false
while ((Get-Date) -lt $deadline) {
    if ($proc.HasExited) {
        Write-Host "[ERROR] Dashboard backend exited early with code $($proc.ExitCode)." -ForegroundColor Red
        Write-Host "Review the console output above. Common fixes: install dashboard dependencies (scripts\setup_dashboard.bat) or check port/configuration."
        exit 1
    }
    if (Test-Health) { $healthy = $true; break }
    Start-Sleep -Milliseconds 500
}

if (-not $healthy) {
    Write-Host "[ERROR] Dashboard backend did not become healthy within 45 seconds." -ForegroundColor Red
    try { if (-not $proc.HasExited) { Stop-Process -Id $proc.Id -Force } } catch {}
    Write-Host "Check backend logs above, verify .venv dependencies, and retry."
    exit 1
}

Write-Host "[OK] Dashboard backend is healthy." -ForegroundColor Green
Write-Host "Opening browser: $url"
Start-Process $url | Out-Null
Write-Host ""
Write-Host "Dashboard is running. Keep this window open for backend logs."
Write-Host "Use 'Start Demo Environment' in the browser to start Kafka/Flink and the streaming job."
Write-Host "Press Ctrl+C to stop the dashboard backend. Infrastructure will not be stopped automatically."
Write-Host ""

try {
    Wait-Process -Id $proc.Id
} catch {}
$code = 0
try { $code = $proc.ExitCode } catch {}
if ($code -ne 0) {
    Write-Host "[ERROR] Dashboard backend exited with code $code." -ForegroundColor Red
}
exit $code
