# Execution Guide

## Prerequisites

- Windows with PowerShell 7+
- Python 3.11
- Docker Desktop
- Shared lab containers running:
  - `flink-jobmanager`
  - `flink-taskmanager`
  - `kafka_kraft_docker`
- Flink REST available at `http://localhost:18081`
- Kafka bootstrap available at `localhost:9092` from the host

Verify:

```powershell
docker ps --format "table {{.Names}}\t{{.Status}}"
```

The project job uses the internal broker address
`kafka_kraft_docker:29092` from inside the Flink containers. Host-side
producers and consumers use `localhost:9092`.

## Python setup

From the repository root:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e .
```

## Topic setup

Create each project topic only if it does not already exist. Do not alter
existing shared topics.

```powershell
docker exec kafka_kraft_docker /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:29092 --create --if-not-exists --topic ecommerce.order-lifecycle.events --partitions 2 --replication-factor 1
docker exec kafka_kraft_docker /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:29092 --create --if-not-exists --topic ecommerce.order-lifecycle.state --partitions 2 --replication-factor 1 --config cleanup.policy=compact
docker exec kafka_kraft_docker /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:29092 --create --if-not-exists --topic ecommerce.order-lifecycle.processed --partitions 2 --replication-factor 1
docker exec kafka_kraft_docker /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:29092 --create --if-not-exists --topic ecommerce.order-lifecycle.exceptions --partitions 2 --replication-factor 1
```

Check topics:

```powershell
docker exec kafka_kraft_docker /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:29092 --list
```

## Submit the Flink job

Copy the Flink job directory and its resources into the JobManager container:

```powershell
docker exec flink-jobmanager mkdir -p /tmp/order-lifecycle/config
docker cp src/order_lifecycle/flink/. flink-jobmanager:/tmp/order-lifecycle/
docker cp config/processing_rules.json flink-jobmanager:/tmp/order-lifecycle/config/processing_rules.json
```

Submit:

```powershell
docker exec flink-jobmanager flink run -d -py /tmp/order-lifecycle/order_lifecycle_job.py
```

List jobs:

```powershell
docker exec flink-jobmanager flink list
```

Get the job ID from the output and check status:

```powershell
$jobId = "<job-id>"
Invoke-RestMethod "http://localhost:18081/jobs/$jobId"
```

The job starts the Kafka source at the latest offsets. Publish demo events only
after it is `RUNNING`.

## Publish scenarios

```powershell
python -m order_lifecycle.kafka.producer --scenario normal_flow
python -m order_lifecycle.kafka.producer --scenario duplicate_events
python -m order_lifecycle.kafka.producer --scenario late_events
python -m order_lifecycle.kafka.producer --scenario out_of_order_events
python -m order_lifecycle.kafka.producer --scenario invalid_events
```

Continuous traffic:

```powershell
python -m order_lifecycle.kafka.producer --scenario continuous --interval 2
```

Stop continuous traffic with `Ctrl+C`.

Finite scenarios use timestamps relative to the current run. If one finite
scenario is followed by another on the same long-lived job, the first
scenario's watermark heartbeat can make the next scenario too late. For clean
scenario-by-scenario validation, cancel and resubmit the job before each finite
scenario. Continuous mode is unaffected.

## Consume outputs

State:

```powershell
python -m order_lifecycle.consumers.state_consumer --from-beginning --max-messages 3 --idle-timeout 10
```

Processed:

```powershell
python -m order_lifecycle.consumers.processed_consumer --from-beginning --max-messages 3 --idle-timeout 10
```

Exceptions and alerts:

```powershell
python -m order_lifecycle.consumers.exceptions_consumer --from-beginning --max-messages 3 --idle-timeout 10
```

Run without `--max-messages` to keep following new records. Run without
`--from-beginning` to start at the latest record.

## Recovery checks

Check checkpoints:

```powershell
Invoke-RestMethod "http://localhost:18081/jobs/$jobId/checkpoints"
```

Restart the TaskManager:

```powershell
docker restart flink-taskmanager
```

Wait until the job returns to `RUNNING` and verify that the latest checkpoint
was restored:

```powershell
docker exec flink-jobmanager flink list
Invoke-RestMethod "http://localhost:18081/jobs/$jobId"
```

Create a savepoint:

```powershell
docker exec flink-jobmanager flink savepoint $jobId file:///opt/flink/usrlib/flink-state/savepoints
```

Stop with a savepoint:

```powershell
docker exec flink-jobmanager flink stop -p file:///opt/flink/usrlib/flink-state/savepoints $jobId
```

Restore from a savepoint:

```powershell
docker exec flink-jobmanager flink run -d -s file:/opt/flink/usrlib/flink-state/savepoints/<savepoint-name> -py /tmp/order-lifecycle/order_lifecycle_job.py
```

Do not use `docker kill flink-taskmanager` for planned tests. Use
`docker restart` so the container restart policy remains effective.

Do not delete checkpoint or savepoint directories while their recovery history
may still be needed.

## Tests

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests\unit -v
```

Expected: **85 tests, OK**.

## Troubleshooting

### Job is not receiving events

- Confirm the job is `RUNNING` before publishing.
- Confirm the source starts at latest and the producer publishes after
  submission.
- Check the input topic has records:

```powershell
docker exec kafka_kraft_docker /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:29092 --topic ecommerce.order-lifecycle.events --from-beginning --max-messages 1 --timeout-ms 10000
```

### Consumers print no records

- Confirm records exist in the target topic.
- Confirm `KAFKA_BOOTSTRAP_SERVERS` points to `localhost:9092`.
- If the shared broker reports an unusable consumer-offset internal topic, the
  consumers fall back to manual partition assignment and print a warning; this
  is expected in the current shared lab.

### Job fails after submission

- Check the JobManager log:

```powershell
docker logs --tail 200 flink-jobmanager
```

- Check the TaskManager log:

```powershell
docker logs --tail 200 flink-taskmanager
```

- Confirm `/tmp/order-lifecycle/lib/order-lifecycle-serializers.jar` exists in
  the container.
- Confirm `/tmp/order-lifecycle/config/processing_rules.json` exists in the
  container.

### Delay alerts do not appear

- Only non-terminal stages schedule alerts.
- Alerts require the configured processing-time threshold to elapse.
- Default thresholds are several minutes; use a short test config or wait for
  the configured interval.
- A pending valid transition or cancellation may intentionally suppress a false
  alert.
