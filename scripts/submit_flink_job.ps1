param(
    [string]$SavepointPath
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$containerJobPath = "/tmp/order-lifecycle"
$jobEntryPoint = "$containerJobPath/order_lifecycle_job.py"

docker exec flink-jobmanager mkdir -p "$containerJobPath/config"
docker cp (Join-Path $repoRoot "src\order_lifecycle\flink\.") "flink-jobmanager:$containerJobPath/"
docker cp (Join-Path $repoRoot "config\processing_rules.json") "flink-jobmanager:$containerJobPath/config/processing_rules.json"

if ($SavepointPath) {
    docker exec flink-jobmanager flink run -d -s $SavepointPath -py $jobEntryPoint
}
else {
    docker exec flink-jobmanager flink run -d -py $jobEntryPoint
}

docker exec flink-jobmanager flink list
