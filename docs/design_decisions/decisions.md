# Design Decisions

## Status

Core lifecycle, heartbeat, Kafka outputs, duration/delay analytics, local checkpoint recovery, independent consumers, and project documentation are implemented for the local portfolio scope.

## Decision log

| ID | Decision | Status | Rationale |
|----|----------|--------|-----------|
| D-001 | Event-time stage and total delivery durations | Implemented | Durations are business/event semantics and must use `event_timestamp`, not processing time. |
| D-002 | Processing-time delay/stuck alerts | Implemented | Operational alerts must fire even without new business events; processing-time timers provide real elapsed monitoring. |
| D-003 | Configurable thresholds in `config/processing_rules.json` | Implemented | Thresholds are business assumptions, not code constants; test configs can override without changing committed defaults. |
| D-004 | Publish alerts to existing exceptions topic | Implemented | Alerts are operational exceptions with explicit `alert_type`/`exception_type`; no new topic required. |
| D-005 | Suppress false alerts when event-time stage already completed | Implemented | Pending in-threshold transitions/cancellations prevent alerting during event-time buffering/out-of-order acceptance. |
| D-006 | Filesystem checkpoints on the shared Docker volume | Implemented | Checkpoints and savepoints must survive container replacement and be visible to both JobManager and TaskManager. |
| D-007 | HashMap state backend for the local deployment | Implemented | Lifecycle state is small and fits on heap; completed checkpoints provide durable recovery. |
| D-008 | Bounded fixed-delay restart strategy | Implemented | Automatic worker-failure recovery with a finite retry limit avoids restart loops on persistent faults. |
| D-009 | Stable managed-state descriptors and timers | Implemented | Stable names and types allow checkpoint/savepoint restoration without skipping state. |
| D-010 | Keep Kafka sinks at-least-once | Implemented | The existing sinks are not transactional; duplicate outputs after recovery are documented and consumers must deduplicate where needed. |
| D-011 | Fall back to manual partition assignment when consumer groups are unavailable | Implemented | The shared single-broker Kafka lab cannot create the internal consumer-offset topic with its configured replication factor. The fallback preserves independent read capability without modifying shared infrastructure. |

See `docs/design_decisions/duration_and_delay_detection.md` for clock semantics, defaults, and timer behavior. See `docs/design_decisions/checkpointing_and_recovery.md` for checkpoint settings, savepoint commands, and recovery semantics.

## Pending decisions

- TODO: Full processed/state/exception schema governance and compatibility rules.
- TODO: RocksDB, remote durable storage, and JobManager HA for production scale.
- TODO: Checkpoint/savepoint retention and cleanup policy.
- TODO: Production observability dashboard/reporting for duration and delay metrics.
- TODO: Transactional or idempotent Kafka sink if end-to-end exactly-once delivery is required.
