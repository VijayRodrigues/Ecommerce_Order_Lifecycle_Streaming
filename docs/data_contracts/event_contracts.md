# Event Contracts

## Versioning

All input and processed records use `schema_version=1.0`. The Flink job rejects
any other version as a malformed event. Additive output fields are allowed in
the processed schema; input remains strict and rejects additional fields.

## Input event contract

Schema: [`schemas/order_event.schema.json`](../../schemas/order_event.schema.json)

Required fields:

| Field | Type | Rules |
|---|---|---|
| `schema_version` | string | Must be `1.0` |
| `event_id` | string | Must be a UUID |
| `order_id` | string | Non-empty |
| `event_type` | string | One of the accepted lifecycle or heartbeat types |
| `event_timestamp` | string | ISO-8601 timestamp with timezone |
| `source` | string | `order_service`, `warehouse`, `delivery_partner`, or `system` |

Accepted `event_type` values:

- `ORDER_PLACED`
- `ORDER_CONFIRMED`
- `ORDER_PACKED`
- `ORDER_SHIPPED`
- `OUT_FOR_DELIVERY`
- `ORDER_DELIVERED`
- `ORDER_CANCELLED`
- `WATERMARK_HEARTBEAT`

Heartbeat events pass validation, participate in timestamp assignment and
watermarks, and are then filtered out before lifecycle processing. They must use
`source=system`.

## Processed event contract

Schema: [`schemas/processed_order.schema.json`](../../schemas/processed_order.schema.json)

Processed records preserve the input fields and add:

| Field | Description |
|---|---|
| `processing_status` | `ACCEPTED` |
| `previous_status` | Previous lifecycle status, or `null` for the first event |
| `current_status` | Accepted lifecycle status |
| `previous_event_timestamp` | Timestamp of the previous accepted event |
| `current_event_timestamp` | Timestamp of the accepted event |
| `stage_duration_seconds` | Event-time duration from the previous accepted event |
| `stage_duration_minutes` | Event-time duration in minutes |
| `processed_at` | Processing timestamp |
| `order_placed_timestamp` | Present on delivered records |
| `order_delivered_timestamp` | Present on delivered records |
| `total_delivery_duration_seconds` | Present on delivered records |
| `total_delivery_duration_minutes` | Present on delivered records |

The first `ORDER_PLACED` has null stage-duration fields because there is no
previous accepted lifecycle event. Total delivery duration is only calculated
for `ORDER_DELIVERED`.

## Current state contract

The state topic uses the same `order_id` key and contains the latest accepted
state per order:

| Field | Description |
|---|---|
| `order_id` | Order identifier |
| `current_status` | Latest accepted status |
| `previous_status` | Status before the latest accepted event |
| `last_event_id` | ID of the latest accepted event |
| `last_event_type` | Type of the latest accepted event |
| `last_event_timestamp` | Event timestamp of the latest accepted event |
| `last_source` | Source of the latest accepted event |
| `previous_event_timestamp` | Previous accepted event timestamp |
| `current_event_timestamp` | Latest accepted event timestamp |
| `stage_duration_seconds` | Latest stage duration |
| `stage_duration_minutes` | Latest stage duration in minutes |
| `updated_at` | Processing timestamp |
| `order_placed_timestamp` | Present after delivery |
| `order_delivered_timestamp` | Present after delivery |
| `total_delivery_duration_seconds` | Present after delivery |
| `total_delivery_duration_minutes` | Present after delivery |

The state topic is compacted. Consumers read the latest retained value per
order, not every historical transition.

## Exceptions contract

All records use the exceptions topic and include `exception_type`,
`exception_category`, and `failed_at` or `alerted_at`.

| `exception_type` | Category | Trigger |
|---|---|---|
| `MALFORMED_EVENT` | `MALFORMED_EVENTS` | Invalid JSON, missing or extra fields, invalid enum, bad UUID, bad timestamp |
| `DUPLICATE_EVENT` | `DUPLICATE_EVENTS` | `event_id` already seen |
| `EVENT_TOO_LATE` | `LIFECYCLE_EXCEPTIONS` | Event timestamp at or before the current watermark |
| `INVALID_LIFECYCLE_TRANSITION` | `LIFECYCLE_EXCEPTIONS` | Event not allowed from the current status |
| `STAGE_DELAYED` | `DELAY_ALERTS` | Processing-time stage threshold exceeded |
| `ORDER_DELIVERY_DELAYED` | `DELAY_ALERTS` | `OUT_FOR_DELIVERY` threshold exceeded |

Malformed and lifecycle records may include partial `order_id` or `event_id`
context when available. Delay alerts include `order_id`, `current_status`,
`stage_started_at`, `stage_started_processing_at`, `elapsed_seconds`,
`threshold_seconds`, and `alerted_at`.

## Compatibility rules

- Input event fields are strict; adding a field to input requires a coordinated
  schema change.
- Output fields may be added without breaking existing consumers, but consumers
  must ignore unknown fields.
- Do not change existing field names or meanings without a version change.
- `exception_type` and `exception_category` remain the routing discriminator
  for consumers.
- Kafka keys remain `order_id` where available and fall back to `event_id`,
  then `unknown`.
