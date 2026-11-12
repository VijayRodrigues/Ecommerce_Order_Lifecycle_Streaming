param(
    [ValidateSet(
        "normal_flow",
        "duplicate_events",
        "late_events",
        "out_of_order_events",
        "invalid_events",
        "continuous"
    )]
    [string]$Scenario = "normal_flow",

    [double]$Interval = 2.0
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$pythonPath = Join-Path $repoRoot ".venv\Scripts\python.exe"

if (-not (Test-Path -LiteralPath $pythonPath)) {
    throw "Virtual environment not found. Run .\scripts\setup.ps1 first."
}

& $pythonPath -m order_lifecycle.kafka.producer --scenario $Scenario --interval $Interval
