
import json
import logging
import uuid
from datetime import datetime

from pyflink.common import Types, WatermarkStrategy
from pyflink.common.time import Duration
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment, OutputTag
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
)
from pyflink.datastream.functions import (
    ProcessFunction,
    KeyedProcessFunction,
)
from pyflink.datastream.state import (
    ValueStateDescriptor,
    ListStateDescriptor,
)


BOOTSTRAP_SERVERS = "kafka_kraft_docker:29092"
INPUT_TOPIC = "ecommerce.order-lifecycle.events"
CONSUMER_GROUP = "order-lifecycle-processor"

ALLOWED_LATENESS_SECONDS = 300

EVENT_TYPES = {
    "ORDER_PLACED",
    "ORDER_CONFIRMED",
    "ORDER_PACKED",
    "ORDER_SHIPPED",
    "OUT_FOR_DELIVERY",
    "ORDER_DELIVERED",
    "ORDER_CANCELLED",
}

SOURCES = {
    "order_service",
    "warehouse",
    "delivery_partner",
}

REQUIRED_FIELDS = {
    "schema_version",
    "event_id",
    "order_id",
    "event_type",
    "event_timestamp",
    "source",
}

EVENT_ORDER = {
    "ORDER_PLACED": 1,
    "ORDER_CONFIRMED": 2,
    "ORDER_PACKED": 3,
    "ORDER_SHIPPED": 4,
    "OUT_FOR_DELIVERY": 5,
    "ORDER_DELIVERED": 6,
    "ORDER_CANCELLED": 7,
}

ALLOWED_TRANSITIONS = {
    None: {"ORDER_PLACED"},
    "ORDER_PLACED": {
        "ORDER_CONFIRMED",
        "ORDER_CANCELLED",
    },
    "ORDER_CONFIRMED": {
        "ORDER_PACKED",
        "ORDER_CANCELLED",
    },
    "ORDER_PACKED": {
        "ORDER_SHIPPED",
        "ORDER_CANCELLED",
    },
    "ORDER_SHIPPED": {
        "OUT_FOR_DELIVERY",
    },
    "OUT_FOR_DELIVERY": {
        "ORDER_DELIVERED",
    },
    "ORDER_DELIVERED": set(),
    "ORDER_CANCELLED": set(),
}

MALFORMED_EVENTS = OutputTag(
    "malformed-events",
    Types.STRING(),
)

DUPLICATE_EVENTS = OutputTag(
    "duplicate-events",
    Types.STRING(),
)

LIFECYCLE_EXCEPTIONS = OutputTag(
    "lifecycle-exceptions",
    Types.STRING(),
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def parse_event_timestamp(timestamp):
    parsed = datetime.fromisoformat(
        timestamp.replace("Z", "+00:00")
    )
    return int(parsed.timestamp() * 1000)


class EventTimestampAssigner(TimestampAssigner):

    def extract_timestamp(self, value, record_timestamp):
        event = json.loads(value)
        return parse_event_timestamp(
            event["event_timestamp"]
        )


class ValidateEvents(ProcessFunction):

    def process_element(self, value, ctx):
        try:
            event = json.loads(value)

            if not isinstance(event, dict):
                raise ValueError("Event must be a JSON object")

            missing_fields = REQUIRED_FIELDS - event.keys()

            if missing_fields:
                raise ValueError(
                    f"Missing required fields: {sorted(missing_fields)}"
                )

            if set(event.keys()) != REQUIRED_FIELDS:
                raise ValueError("Unexpected additional fields")

            if event["schema_version"] != "1.0":
                raise ValueError("Unsupported schema_version")

            if not isinstance(event["event_id"], str):
                raise ValueError("event_id must be a string")

            try:
                uuid.UUID(event["event_id"])
            except (ValueError, AttributeError):
                raise ValueError("Invalid event_id UUID")

            if (
                not isinstance(event["order_id"], str)
                or not event["order_id"].strip()
            ):
                raise ValueError("Invalid order_id")

            if event["event_type"] not in EVENT_TYPES:
                raise ValueError("Invalid event_type")

            if event["source"] not in SOURCES:
                raise ValueError("Invalid source")

            timestamp = event["event_timestamp"]

            if not isinstance(timestamp, str):
                raise ValueError("event_timestamp must be a string")

            try:
                parsed_timestamp = datetime.fromisoformat(
                    timestamp.replace("Z", "+00:00")
                )
            except ValueError:
                raise ValueError("Invalid event_timestamp")

            if parsed_timestamp.tzinfo is None:
                raise ValueError(
                    "event_timestamp must include timezone"
                )

            yield json.dumps(event)

        except (json.JSONDecodeError, ValueError, TypeError) as error:
            malformed = {
                "raw_event": value,
                "error": str(error),
            }

            yield MALFORMED_EVENTS, json.dumps(malformed)


class DeduplicateEvents(KeyedProcessFunction):

    def open(self, runtime_context):
        descriptor = ValueStateDescriptor(
            "event_seen",
            Types.BOOLEAN(),
        )

        self.event_seen = runtime_context.get_state(descriptor)

    def process_element(self, value, ctx):
        event = json.loads(value)
        event_id = event["event_id"]

        if self.event_seen.value() is not None:
            duplicate = {
                "event_id": event_id,
                "order_id": event["order_id"],
                "event_type": event["event_type"],
                "status": "DUPLICATE",
            }

            yield DUPLICATE_EVENTS, json.dumps(duplicate)

        else:
            self.event_seen.update(True)

            yield json.dumps({
                **event,
                "processing_status": "ACCEPTED",
            })


class ValidateLifecycle(KeyedProcessFunction):

    def open(self, runtime_context):
        status_descriptor = ValueStateDescriptor(
            "current_order_status",
            Types.STRING(),
        )

        pending_descriptor = ListStateDescriptor(
            "pending_order_events",
            Types.STRING(),
        )

        self.current_status = runtime_context.get_state(
            status_descriptor
        )

        self.pending_events = runtime_context.get_list_state(
            pending_descriptor
        )

    def process_element(self, value, ctx):
        event = json.loads(value)

        event_timestamp = parse_event_timestamp(
            event["event_timestamp"]
        )

        current_watermark = (
            ctx.timer_service().current_watermark()
        )

        if event_timestamp <= current_watermark:
            exception = {
                "event": event,
                "error": "EVENT_TOO_LATE",
                "current_watermark": current_watermark,
            }

            yield LIFECYCLE_EXCEPTIONS, json.dumps(exception)
            return

        self.pending_events.add(value)

        ctx.timer_service().register_event_time_timer(
            event_timestamp
        )

    def on_timer(self, timestamp, ctx):
        pending = list(self.pending_events.get())

        due_events = []
        remaining_events = []

        for value in pending:
            event = json.loads(value)

            event_timestamp = parse_event_timestamp(
                event["event_timestamp"]
            )

            if event_timestamp <= timestamp:
                due_events.append(event)
            else:
                remaining_events.append(value)

        due_events.sort(
            key=lambda event: (
                parse_event_timestamp(event["event_timestamp"]),
                EVENT_ORDER[event["event_type"]],
            )
        )

        self.pending_events.update(remaining_events)

        for event in due_events:
            previous_status = self.current_status.value()
            event_type = event["event_type"]

            allowed = ALLOWED_TRANSITIONS.get(
                previous_status,
                set(),
            )

            if event_type not in allowed:
                exception = {
                    "event": event,
                    "error": "INVALID_LIFECYCLE_TRANSITION",
                    "previous_status": previous_status,
                    "attempted_event": event_type,
                }

                yield (
                    LIFECYCLE_EXCEPTIONS,
                    json.dumps(exception),
                )

                continue

            self.current_status.update(event_type)

            result = {
                **event,
                "processing_status": "ACCEPTED",
                "previous_status": previous_status,
                "current_status": event_type,
            }

            yield json.dumps(result)


def main():

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP_SERVERS)
        .set_topics(INPUT_TOPIC)
        .set_group_id(CONSUMER_GROUP)
        .set_starting_offsets(
            KafkaOffsetsInitializer.latest()
        )
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    raw_events = env.from_source(
        kafka_source,
        WatermarkStrategy.no_watermarks(),
        "Order Lifecycle Kafka Source",
    )

    validated_events = raw_events.process(
        ValidateEvents(),
        output_type=Types.STRING(),
    )

    malformed_events = validated_events.get_side_output(
        MALFORMED_EVENTS
    )

    deduplicated_events = (
        validated_events
        .key_by(
            lambda value: json.loads(value)["event_id"]
        )
        .process(
            DeduplicateEvents(),
            output_type=Types.STRING(),
        )
    )

    duplicate_events = deduplicated_events.get_side_output(
        DUPLICATE_EVENTS
    )

    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(
            Duration.of_seconds(ALLOWED_LATENESS_SECONDS)
        )
        .with_timestamp_assigner(
            EventTimestampAssigner()
        )
    )

    event_time_events = (
        deduplicated_events
        .assign_timestamps_and_watermarks(
            watermark_strategy
        )
    )

    lifecycle_results = (
        event_time_events
        .key_by(
            lambda value: json.loads(value)["order_id"]
        )
        .process(
            ValidateLifecycle(),
            output_type=Types.STRING(),
        )
    )

    lifecycle_exceptions = lifecycle_results.get_side_output(
        LIFECYCLE_EXCEPTIONS
    )

    lifecycle_results.print("LIFECYCLE_RESULTS")
    lifecycle_exceptions.print("LIFECYCLE_EXCEPTIONS")
    malformed_events.print("MALFORMED_EVENTS")
    duplicate_events.print("DUPLICATE_EVENTS")

    env.execute("Ecommerce Order Lifecycle Processor")


if __name__ == "__main__":
    main()
