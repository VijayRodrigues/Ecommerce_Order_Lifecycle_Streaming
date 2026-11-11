# Duration and Delay Detection

## Event-time stage durations

For every accepted lifecycle transition, the Flink job calculates the stage duration from the immediately preceding accepted lifecycle event using **event timestamps** (`event_timestamp`), not processing timestamps.

- `previous_event_timestamp`: event timestamp of the previous accepted lifecycle event, or `null` for the first `ORDER_PLACED`.
- `current_event_timestamp`: event timestamp of the accepted transition.
- `stage_duration_seconds` / `stage_duration_minutes`: event-time duration between the two timestamps, or `null` for the first `ORDER_PLACED`.

These fields are added to processed-event records (`ecommerce.order-lifecycle.processed`) and state-update records (`ecommerce.order-lifecycle.state`).

## Event-time total delivery duration

When `ORDER_DELIVERED` is accepted, the job calculates total delivery duration from `ORDER_PLACED.event_timestamp` to `ORDER_DELIVERED.event_timestamp` using event time:

- `order_placed_timestamp`
- `order_delivered_timestamp`
- `total_delivery_duration_seconds`
- `total_delivery_duration_minutes`

These fields are included in the processed `ORDER_DELIVERED` event and the final state record. They are not calculated for cancelled or incomplete orders.

## Configurable delay thresholds

Stage-delay thresholds are configured in `config/processing_rules.json` (validated by `src/order_lifecycle/flink/order_lifecycle_job.py` and `schemas/processing_rules.schema.json`).

Default illustrative thresholds (seconds), not universal industry standards:

| Stage | Threshold |
|---|---:|
| `ORDER_PLACED -> ORDER_CONFIRMED` | 900 |
| `ORDER_CONFIRMED -> ORDER_PACKED` | 3600 |
| `ORDER_PACKED -> ORDER_SHIPPED` | 7200 |
| `ORDER_SHIPPED -> OUT_FOR_DELIVERY` | 14400 |
| `OUT_FOR_DELIVERY -> ORDER_DELIVERED` | 10800 |

Thresholds are loaded from `ORDER_LIFECYCLE_PROCESSING_RULES`, a local/container config path, or project `config/processing_rules.json`. Integration tests may use a temporary short-threshold config without changing the committed production defaults.

## Processing-time delay alerts

Delay alerts are operational and use **processing-time timers** so an alert can fire even when no new business event arrives.

- `stage_started_at`: event timestamp of the accepted stage (business/event time context).
- `stage_started_processing_at`: processing time when the stage was accepted by the job.
- `elapsed_seconds`: real processing-time elapsed since `stage_started_processing_at`.
- `threshold_seconds`: configured processing-time threshold for the stage.

Event-time durations and processing-time alerts intentionally use different clocks and are not mixed in calculations. Alert records include both `stage_started_at` and `stage_started_processing_at` to make the distinction explicit.

Alert types:

- `STAGE_DELAYED` for `ORDER_PLACED`, `ORDER_CONFIRMED`, `ORDER_PACKED`, and `ORDER_SHIPPED`.
- `ORDER_DELIVERY_DELAYED` for `OUT_FOR_DELIVERY`.

Alerts are published to the existing `ecommerce.order-lifecycle.exceptions` topic with `exception_type`/`alert_type`, `exception_category=DELAY_ALERTS`, and key `order_id`.

## Timer and correctness behavior

- A processing-time alert timer is scheduled when a non-terminal stage is accepted and cancelled when the order advances.
- At most one alert is emitted per stage (`stage_alert_sent` state).
- If an alert fires after the order has progressed, the emitted alert remains in the exceptions topic; current order state continues to update normally.
- Terminal states (`ORDER_DELIVERED`, `ORDER_CANCELLED`) do not schedule or emit alerts.
- Watermark heartbeat events are filtered before lifecycle processing and do not alter order state or delay timers.
- To avoid false alerts for out-of-order/event-time buffering, an alert is suppressed if a pending valid transition (or cancellation) would complete the stage within the configured event-time threshold.

Existing event-time ordering, 5-minute bounded out-of-orderness, watermark heartbeats, deduplication, malformed-event handling, late-event isolation, lifecycle validation, cancellation rules, and Kafka keys/outputs are preserved.
