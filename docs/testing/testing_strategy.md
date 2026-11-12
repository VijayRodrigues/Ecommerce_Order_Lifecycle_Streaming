# Testing Strategy

## Status

Unit tests are automated and passing. Integration and end-to-end validation
for the local Docker lab is documented as a manual procedure because the shared
infrastructure is persistent and is not recreated by the test suite.

## Test levels

### Unit tests

Location: `tests/unit/`

Run:

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests\unit -v
```

Current result: **85 tests, OK**.

Coverage includes:

- event contract and lifecycle transition rules
- deduplication behavior
- event-time ordering and watermark heartbeats
- malformed, late, duplicate, and invalid-transition output
- stage and total delivery durations
- delay alert thresholds and timer suppression
- Kafka output record keys and payloads
- checkpoint configuration, state names, and restart strategy
- consumer JSON parsing, formatting, configuration, and group defaults
- consumer manual-assignment fallback

The Flink job imports PyFlink inside the container. Local unit tests use the
stubs in `tests/unit/pyflink_stubs.py` where needed.

### Integration tests

Location: `tests/integration/`

The validated integration procedure is documented in
[`docs/demo/demo_guide.md`](../demo/demo_guide.md). It covers:

- creating or verifying project topics
- submitting the PyFlink job
- publishing all five scenarios
- consuming state, processed, and exception records
- restarting the TaskManager and verifying checkpoint restoration
- savepoint stop and restore

A future disposable environment can automate these checks without touching the
shared lab.

### End-to-end tests

Location: `tests/e2e/`

The end-to-end path is demonstrated manually from producer through Kafka,
PyFlink, and the independent consumers. Do not run destructive infrastructure
tests against the shared lab.

## Test data

- Scenario events are generated deterministically in shape, with random
  `order_id` and `event_id` values.
- Fixed past timestamps are used for generated events so Kafka sink timestamp
  checks and event-time windows behave normally.
- Finite scenarios append a watermark heartbeat.
- The demo can use the committed production thresholds or a temporary
  short-threshold config copied into the Flink container. Committed defaults
  are not modified for testing.

## Scenario coverage

| Scenario | Automated unit coverage | Manual end-to-end |
|---|---|---|
| Normal flow | Yes | Yes |
| Duplicate events | Yes | Yes |
| Late events | Yes | Yes |
| Out-of-order events | Yes | Yes |
| Invalid events | Yes | Yes |
| Delay alerts | Yes | Yes, with configured threshold |
| Checkpoint recovery | Configuration and state tests | Yes |
| Savepoint recovery | Configuration tests | Yes |

## Tooling

- Python `unittest`
- PyFlink stubs for local imports
- Docker CLI for shared Kafka and Flink checks
- Kafka CLI tools inside `kafka_kraft_docker`
- Flink REST API at `http://localhost:18081`
- Project consumers for record-level output verification

## Quality gates

A change is ready when:

1. The unit suite passes.
2. The Flink job submits successfully.
3. At least one normal scenario reaches all three output topics.
4. The consumers display the expected record types.
5. Recovery behavior is unchanged if checkpoint or state code was modified.
6. No shared infrastructure or existing topic configuration was altered.
