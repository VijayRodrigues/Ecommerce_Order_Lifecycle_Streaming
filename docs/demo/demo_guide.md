# Demonstration Guide

## Goal

Demonstrate the complete path from synthetic order events to Kafka outputs,
including abnormal stream conditions, independent consumers, and Flink
recovery.

The demo uses the shared Docker lab and does not modify infrastructure or
existing topics.

## Preparation

From the repository root:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e .
```

Verify:

```powershell
docker ps --format "table {{.Names}}\t{{.Status}}"
docker exec flink-jobmanager flink list
```

If the job is already running with the production configuration, keep it. To
start a fresh job, follow the submission steps below.

## Demo flow

### 1. Submit the job

```powershell
docker exec flink-jobmanager mkdir -p /tmp/order-lifecycle/config
docker cp src/order_lifecycle/flink/. flink-jobmanager:/tmp/order-lifecycle/
docker cp config/processing_rules.json flink-jobmanager:/tmp/order-lifecycle/config/processing_rules.json
docker exec flink-jobmanager flink run -d -py /tmp/order-lifecycle/order_lifecycle_job.py
```

Record the job ID:

```powershell
docker exec flink-jobmanager flink list
```

Expected result: `Ecommerce Order Lifecycle Processor (RUNNING)`.

The source starts at latest offsets, so always submit before publishing demo
events.

### 2. Verify topics and consumers

List topics:

```powershell
docker exec kafka_kraft_docker /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:29092 --list
```

Each consumer can be demonstrated independently. For example, in a separate
terminal:

```powershell
python -m order_lifecycle.consumers.processed_consumer --from-beginning --max-messages 5 --idle-timeout 15
```

The state consumer prints a compaction note because the state topic retains the
latest value per order rather than a complete event history.

### Scenario isolation

Finite scenarios use event timestamps relative to the current run and add a
watermark heartbeat. After one scenario, the watermark can move beyond the
default start time of the next scenario, causing the next scenario to be
reported as `EVENT_TOO_LATE`.

For a clean scenario-by-scenario demonstration, submit a fresh job before each
finite scenario and record the new job ID:

```powershell
docker exec flink-jobmanager flink cancel <job-id>
docker cp src/order_lifecycle/flink/. flink-jobmanager:/tmp/order-lifecycle/
docker cp config/processing_rules.json flink-jobmanager:/tmp/order-lifecycle/config/processing_rules.json
docker exec flink-jobmanager flink run -d -py /tmp/order-lifecycle/order_lifecycle_job.py
docker exec flink-jobmanager flink list
```

If you are demonstrating only one finite scenario, no restart is required.
Continuous mode is unaffected because new generated events advance with
wall-clock time.

### 3. Normal flow

```powershell
python -m order_lifecycle.kafka.producer --scenario normal_flow
```

Expected:

- processed topic contains six accepted lifecycle events for one order
- state topic contains the latest state for that order
- delivered state includes total delivery duration
- no exceptions are required for the scenario

Read the result:

```powershell
python -m order_lifecycle.consumers.processed_consumer --from-beginning --max-messages 6 --idle-timeout 15
python -m order_lifecycle.consumers.state_consumer --from-beginning --max-messages 6 --idle-timeout 15
```

The state consumer may show fewer than six records per order because the topic
is compacted and the displayed records are the retained latest values.

### 4. Duplicate events

```powershell
python -m order_lifecycle.kafka.producer --scenario duplicate_events
```

Expected:

- the repeated `event_id` is not accepted a second time
- a `DUPLICATE_EVENT` record is written to the exceptions topic
- lifecycle state does not advance twice for the duplicate

```powershell
python -m order_lifecycle.consumers.exceptions_consumer --from-beginning --max-messages 5 --idle-timeout 15
```

Look for `exception_type: DUPLICATE_EVENT` and
`exception_category: DUPLICATE_EVENTS`.

### 5. Late events

```powershell
python -m order_lifecycle.kafka.producer --scenario late_events
```

The generator introduces an event after the watermark has advanced. Expected:

- the late event is isolated rather than silently applied out of order
- an `EVENT_TOO_LATE` record may appear for the affected event
- the remaining valid lifecycle events continue to process

```powershell
python -m order_lifecycle.consumers.exceptions_consumer --from-beginning --max-messages 5 --idle-timeout 15
```

### 6. Out-of-order events

```powershell
python -m order_lifecycle.kafka.producer --scenario out_of_order_events
```

Expected:

- events are buffered by `order_id` until event time advances
- accepted transitions are emitted in lifecycle order
- stage durations use event timestamps, not arrival order

### 7. Invalid events

```powershell
python -m order_lifecycle.kafka.producer --scenario invalid_events
```

Expected:

- malformed JSON or missing fields produce `MALFORMED_EVENT`
- an invalid lifecycle transition produces
  `INVALID_LIFECYCLE_TRANSITION`
- valid events in the scenario continue to process

```powershell
python -m order_lifecycle.consumers.exceptions_consumer --from-beginning --max-messages 10 --idle-timeout 15
```

### 8. Delay alerts

Delay alerts are based on processing-time timers and the thresholds in
`config/processing_rules.json`. Default thresholds are several minutes, so a
full delayed-stage demo can take time.

For a short local check, submit the job with a temporary short-threshold config
instead of changing the committed production defaults. The job resolves the
config path from:

1. `ORDER_LIFECYCLE_PROCESSING_RULES`
2. `/tmp/order-lifecycle/config/processing_rules.json`
3. the current working directory

A short test config is only for demonstration and must not replace the
committed production configuration.

Expected alert records:

- `STAGE_DELAYED` for non-terminal stages
- `ORDER_DELIVERY_DELAYED` for `OUT_FOR_DELIVERY`
- `exception_category=DELAY_ALERTS`
- `elapsed_seconds` and `threshold_seconds` populated

### 9. Continuous traffic

In one terminal:

```powershell
python -m order_lifecycle.kafka.producer --scenario continuous --interval 2
```

In other terminals:

```powershell
python -m order_lifecycle.consumers.state_consumer
python -m order_lifecycle.consumers.processed_consumer
python -m order_lifecycle.consumers.exceptions_consumer
```

Stop with `Ctrl+C`. The consumers follow new records and do not need to be
restarted when scenarios change.

## Recovery demonstration

### Checkpoints

```powershell
$jobId = "<job-id>"
Invoke-RestMethod "http://localhost:18081/jobs/$jobId/checkpoints"
```

A completed checkpoint appears after roughly one minute of job runtime.

### TaskManager restart

```powershell
docker restart flink-taskmanager
```

Then:

```powershell
docker exec flink-jobmanager flink list
Invoke-RestMethod "http://localhost:18081/jobs/$jobId"
```

Expected:

- the job returns to `RUNNING`
- the latest completed checkpoint is restored
- buffered out-of-order state and timers survive
- previously seen event IDs remain deduplicated

Publish the same event ID again and check the exceptions consumer for a
`DUPLICATE_EVENT`.

### Savepoint

```powershell
docker exec flink-jobmanager flink savepoint $jobId file:///opt/flink/usrlib/flink-state/savepoints
```

Or stop and save in one step:

```powershell
docker exec flink-jobmanager flink stop -p file:///opt/flink/usrlib/flink-state/savepoints $jobId
```

Restore:

```powershell
docker exec flink-jobmanager flink run -d -s file:/opt/flink/usrlib/flink-state/savepoints/<savepoint-name> -py /tmp/order-lifecycle/order_lifecycle_job.py
```

Expected: a new job ID restores the saved state and continues processing.
Kafka output remains at-least-once, so duplicate output records are possible
after recovery.

## Expected output shapes

### State record

```text
ORDER STATE
  order_id: ORD-...
  current_status: ORDER_DELIVERED
  previous_status: OUT_FOR_DELIVERY
  stage_duration_seconds: 600
  total_delivery_duration_seconds: 3000
```

### Processed record

```text
PROCESSED EVENT
  order_id: ORD-...
  event_type: ORDER_DELIVERED
  current_status: ORDER_DELIVERED
  processing_status: ACCEPTED
```

### Exception record

```text
EXCEPTION / ALERT
  exception_type: DUPLICATE_EVENT
  exception_category: DUPLICATE_EVENTS
  order_id: ORD-...
  error: Event with this event_id was already processed
```

## Cleanup

Stop continuous producers and consumers with `Ctrl+C`.

If the demonstration created a disposable Flink job and it is no longer
needed:

```powershell
docker exec flink-jobmanager flink cancel <job-id>
```

Do not delete shared checkpoint or savepoint directories unless the recovery
history is no longer needed.
