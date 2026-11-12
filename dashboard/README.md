# Streaming Control Centre Dashboard

A local web control centre for the `Ecommerce_Order_Lifecycle_Streaming` pipeline. It uses the existing Kafka topics, PyFlink job, generator, DockerLab infrastructure, and Flink REST API. No React build, database, Redis, cloud service, or extra Kafka/Flink cluster is required.

## Architecture

- **Backend:** FastAPI + Uvicorn (`dashboard/backend/app/main.py`)
- **Frontend:** Static HTML, CSS, and vanilla JavaScript (`dashboard/frontend/`)
- **Live data:** three dedicated dashboard Kafka monitors read the existing output topics:
  - `dashboard-order-state-monitor` → `ecommerce.order-lifecycle.state`
  - `dashboard-processed-events-monitor` → `ecommerce.order-lifecycle.processed`
  - `dashboard-exceptions-monitor` → `ecommerce.order-lifecycle.exceptions`
- **Monitoring/control path:** Docker CLI via allowlisted subprocess calls, Flink REST API, managed generator subprocesses, WebSocket broadcasts.
- **State model:** bounded in-memory stores only. State topic is treated as compacted latest-known state; processed events provide lifecycle history; no full historical reconstruction is claimed beyond observed/retained records.

## Prerequisites

- Windows with Docker Desktop / Docker Engine available
- Shared DockerLab infrastructure at `F:\DockerLab` (override with `DOCKERLAB_ROOT`)
- Existing project `.venv` with project dependencies
- Kafka container `kafka_kraft_docker` and Flink containers `flink-jobmanager`, `flink-taskmanager`
- Flink REST API on `http://localhost:18081` (override with `FLINK_JOBMANAGER_REST_URL`)

## Setup

From the project root:

```bat
scripts\setup_dashboard.bat
```

Or manually:

```bat
.\.venv\Scripts\python.exe -m pip install -r dashboard\backend\requirements.txt
```

## Start / Stop

Start:

```bat
run_dashboard.bat
```

Open: [http://127.0.0.1:8090](http://127.0.0.1:8090)

Stop: press `Ctrl+C` in the dashboard terminal.

## Configuration

Environment variables (all optional):

- `DASHBOARD_HOST` (default `127.0.0.1`)
- `DASHBOARD_PORT` (default `8090`)
- `DOCKERLAB_ROOT` (default `F:\DockerLab`)
- `KAFKA_BOOTSTRAP_SERVERS` (default `localhost:9092`)
- `FLINK_JOBMANAGER_REST_URL` (default `http://localhost:18081`)
- `FLINK_JOB_NAME` (default `Ecommerce Order Lifecycle Processor`)
- `KAFKA_INPUT_TOPIC`, `KAFKA_STATE_TOPIC`, `KAFKA_PROCESSED_TOPIC`, `KAFKA_EXCEPTIONS_TOPIC`
- `DASHBOARD_STATE_CONSUMER_GROUP`, `DASHBOARD_PROCESSED_CONSUMER_GROUP`, `DASHBOARD_EXCEPTIONS_CONSUMER_GROUP`
- `DASHBOARD_KAFKA_FROM_BEGINNING` (`true`/`false`, default `true`)
- Buffer/cache sizes and command timeout: `DASHBOARD_*`

## Controls and Safety

- Infrastructure start uses only allowlisted `docker compose -f ... up -d` for the project Kafka/Flink compose files.
- Infrastructure stop uses `docker stop` for `kafka_kraft_docker`, `flink-taskmanager`, `flink-jobmanager` only. No `docker compose down`, no volume deletion, no checkpoint/savepoint removal.
- Flink submit copies the existing job/config into JobManager and runs `flink run -d -py ...` only when a project job is not already running.
- Flink stop cancels only the current project job via Flink REST (`PATCH /jobs/{id}?mode=cancel`), never the cluster.
- Generator controls run the existing `order_lifecycle.kafka.producer` scenarios as managed subprocesses using the project `.venv`.

## Kafka/Flink Communication

- **Kafka:** `kafka-python` admin/consumer clients to `localhost:9092`. Dashboard monitors use dedicated groups and fall back to manual partition assignment if group coordination is unavailable.
- **Flink:** HTTP requests to `/overview`, `/jobs/overview`, `/jobs/{id}/checkpoints`, and job cancel endpoint.
- **Frontend:** REST polling for status/logs plus WebSocket `/ws/live` for processed/state/exception/metrics/activity updates.

## Limitations

- Monitoring history is in-memory and bounded; restarting the dashboard resets counters/history except records re-read from Kafka when configured.
- Kafka sinks are at-least-once; duplicate processed records can appear after recovery.
- Delay alerts depend on Flink processing-time timers and configured thresholds; they may require waiting.
- Finite scenarios can produce `EVENT_TOO_LATE` on a long-lived job after prior finite runs due to watermark advancement; use a fresh job for clean finite scenario demos.
- This is a localhost demonstration app, not a hardened multi-user production control plane.

## Troubleshooting

- **Pipeline unavailable:** verify Docker containers, Kafka bootstrap `localhost:9092`, and Flink REST `http://localhost:18081`.
- **No live records:** check dashboard consumer status in Infrastructure, confirm Flink job is running, and run a generator scenario.
- **Flink submit failed:** ensure `src/order_lifecycle/flink` and `config/processing_rules.json` exist and JobManager container is running.
- **Generator errors:** check `.venv` dependencies and generator logs in the dashboard; run `python -m order_lifecycle.kafka.producer --scenario normal_flow` manually for diagnosis.
- **Topic/group issues:** dashboard falls back to manual assignment when `__consumer_offsets` coordination is unavailable in single-broker setups.
