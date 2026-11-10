# Business Requirements

## 1. Project context

**Project:** E-commerce Order Lifecycle and Event Processing

**Repository:** `Ecommerce_Order_Lifecycle_Streaming`

## 2. Business objective

Process e-commerce order lifecycle events as a continuous stream so that the
order lifecycle can be validated and analysed reliably, even when events are
duplicated, late, out of order, or invalid.

## 3. Established functional requirements

### 3.1 Event ingestion

- The system must ingest order lifecycle events from a streaming source.
- Events must be associated with an existing order lifecycle.
- Each lifecycle stage must be represented explicitly by an event.

### 3.2 Lifecycle processing

- The system must process order lifecycle events in the context of the order
  they belong to.
- The system must determine and track the current lifecycle state of an order.
- Lifecycle durations between stages must be calculable.
- Delayed lifecycle transitions must be detectable.

### 3.3 Event-stream conditions

The system must explicitly handle the following conditions:

- **Normal flow** — events arrive as expected.
- **Duplicate events** — the same event is received more than once and must not
  be processed as a new lifecycle transition.
- **Late events** — events arrive after the expected point in the lifecycle.
- **Out-of-order events** — events arrive in an order different from their
  event time.
- **Invalid events** — events that do not satisfy the data contract or
  lifecycle rules.

### 3.4 Outputs

- The system must produce records representing the processed state of an order.
- Processed outputs must be separable from raw inputs so that results can be
  validated and analysed.

## 4. Established constraints

- The project must be fully independent of the other e-commerce portfolio
  projects.
- All source code, configuration, tests, documentation, generated data and
  sample data must reside within this repository.
- Shared infrastructure may be reused but must not be modified.
- No infrastructure may be reinstalled.
- No additional Docker services or Compose changes are permitted.
- No existing Kafka topics may be altered.
- Project-specific topics and configuration must be maintained independently.

## 5. Pending requirements

TODO: Confirm and document the following with the business owner.

- Exact order lifecycle stages and allowed transitions.
- Required event fields and identifier rules.
- Definition of a duplicate event.
- Allowed lateness window and out-of-order tolerance.
- Delay thresholds and severity levels.
- Validation failure handling and dead-letter expectations.
- Required processed-order output fields.
- Non-functional requirements (throughput, latency, retention).
- Reporting and analytics expectations.
