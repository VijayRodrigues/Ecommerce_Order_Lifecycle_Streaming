# E-commerce Order Lifecycle and Event Processing

GitHub repository: `Ecommerce_Order_Lifecycle_Streaming`

## Business objective

Process e-commerce order lifecycle events as a continuous stream so that order
state transitions can be validated, deduplicated, correctly ordered, and
analysed for lifecycle durations and processing delays.

The project must correctly handle realistic event-stream conditions, including:

- normal order flows
- duplicate events
- late-arriving events
- out-of-order events
- invalid events

Detailed business requirements are maintained in
[`docs/business_requirements/requirements.md`](docs/business_requirements/requirements.md).

## Technology direction

- **Language:** Python
- **Stream processing:** Apache Flink 2.2.1 with PyFlink
- **Messaging:** Apache Kafka
- **Runtime:** Java 17
- **Kafka-Flink connector:** 5.0.0-2.2
- **Local infrastructure:** shared Docker-based lab (`F:\DockerLab`), reused
  without modification

> The technology direction is indicative at this stage. No implementation
> decisions are final until recorded in `docs/design_decisions/decisions.md`.

## Current development status

**Scaffolding complete — implementation not started.**

At present the repository contains only the initial project structure,
configuration placeholders, schema placeholders, and documentation
placeholders. The following remain pending:

- business rule finalisation
- architecture design
- data contract definition
- event generator implementation
- Kafka producer implementation
- PyFlink processing implementation
- test implementation

No Kafka producer, PyFlink job, event generator, business logic, or tests have
been implemented yet.
