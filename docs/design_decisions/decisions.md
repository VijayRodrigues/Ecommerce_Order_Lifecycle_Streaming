# Design Decisions

## Status

Core lifecycle, heartbeat, Kafka outputs, and duration/delay analytics are implemented incrementally.

## Decision log

| ID | Decision | Status | Rationale |
|----|----------|--------|-----------|
| D-001 | Event-time stage and total delivery durations | Implemented | Durations are business/event semantics and must use `event_timestamp`, not processing time. |
| D-002 | Processing-time delay/stuck alerts | Implemented | Operational alerts must fire even without new business events; processing-time timers provide real elapsed monitoring. |
| D-003 | Configurable thresholds in `config/processing_rules.json` | Implemented | Thresholds are business assumptions, not code constants; test configs can override without changing committed defaults. |
| D-004 | Publish alerts to existing exceptions topic | Implemented | Alerts are operational exceptions with explicit `alert_type`/`exception_type`; no new topic required. |
| D-005 | Suppress false alerts when event-time stage already completed | Implemented | Pending in-threshold transitions/cancellations prevent alerting during event-time buffering/out-of-order acceptance. |

See `docs/design_decisions/duration_and_delay_detection.md` for clock semantics, defaults, and timer behavior.

## Pending decisions

- TODO: Full processed/state/exception schema governance and compatibility rules.
- TODO: State backend/checkpoint tuning for long-running deployments.
- TODO: Production observability dashboard/reporting for duration and delay metrics.
