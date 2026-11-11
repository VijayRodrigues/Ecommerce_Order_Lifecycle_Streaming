
import json
import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path

from pyflink.common import Row, Types, WatermarkStrategy
from pyflink.common.time import Duration
from pyflink.common.serialization import (
    SerializationSchema,
    SimpleStringSchema,
)
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment, OutputTag
from pyflink.datastream.connectors.base import DeliveryGuarantee
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
    KafkaSink,
    KafkaRecordSerializationSchema,
)
from pyflink.datastream.functions import (
    ProcessFunction,
    KeyedProcessFunction,
)
from pyflink.datastream.state import (
    ValueStateDescriptor,
    ListStateDescriptor,
)
from pyflink.java_gateway import get_gateway


BOOTSTRAP_SERVERS = "kafka_kraft_docker:29092"
INPUT_TOPIC = "ecommerce.order-lifecycle.events"
CONSUMER_GROUP = "order-lifecycle-processor"
STATE_TOPIC = "ecommerce.order-lifecycle.state"
PROCESSED_TOPIC = "ecommerce.order-lifecycle.processed"
EXCEPTIONS_TOPIC = "ecommerce.order-lifecycle.exceptions"

SERIALIZER_JAR_PATH = (
    Path(__file__).resolve().parent
    / "lib"
    / "order-lifecycle-serializers.jar"
)

KAFKA_RECORD_TYPE = Types.ROW_NAMED(
    ["kafka_key", "payload"],
    [Types.STRING(), Types.STRING()],
)

ALLOWED_LATENESS_SECONDS = 300

WATERMARK_HEARTBEAT = "WATERMARK_HEARTBEAT"

EVENT_TYPES = {
    "ORDER_PLACED",
    "ORDER_CONFIRMED",
    "ORDER_PACKED",
    "ORDER_SHIPPED",
    "OUT_FOR_DELIVERY",
    "ORDER_DELIVERED",
    "ORDER_CANCELLED",
    "WATERMARK_HEARTBEAT",
}

SOURCES = {
    "order_service",
    "warehouse",
    "delivery_partner",
    "system",
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

STATE_UPDATES = OutputTag(
    "state-updates",
    Types.STRING(),
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def parse_event_timestamp(timestamp):
    parsed = datetime.fromisoformat(
        timestamp.replace("Z", "+00:00")
    )
    return int(parsed.timestamp() * 1000)


def is_business_event(value):
    return json.loads(value)["event_type"] != WATERMARK_HEARTBEAT


def utc_now_iso():
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def extract_partial_ids(raw_event):
    try:
        event = json.loads(raw_event)
    except (json.JSONDecodeError, TypeError):
        return {}

    if not isinstance(event, dict):
        return {}

    partial_ids = {}

    for field in ("order_id", "event_id"):
        value = event.get(field)

        if isinstance(value, str) and value.strip():
            partial_ids[field] = value

    return partial_ids


def select_record_key(record):
    if not isinstance(record, dict):
        return "unknown"

    for field in ("order_id", "event_id"):
        value = record.get(field)

        if isinstance(value, str) and value.strip():
            return value

    return "unknown"


def to_kafka_record(value):
    try:
        record = json.loads(value)
    except (json.JSONDecodeError, TypeError):
        record = {}

    return Row(select_record_key(record), value)


def build_state_update(event, previous_status, current_status):
    return {
        "order_id": event["order_id"],
        "current_status": current_status,
        "previous_status": previous_status,
        "last_event_id": event["event_id"],
        "last_event_type": event["event_type"],
        "last_event_timestamp": event["event_timestamp"],
        "last_source": event["source"],
        "updated_at": utc_now_iso(),
    }


def build_processed_event(event, previous_status, current_status):
    return {
        **event,
        "processing_status": "ACCEPTED",
        "previous_status": previous_status,
        "current_status": current_status,
        "processed_at": utc_now_iso(),
    }


def build_malformed_exception(raw_event, error):
    exception = {
        "exception_type": "MALFORMED_EVENT",
        "exception_category": "MALFORMED_EVENTS",
        "error": error,
        "raw_event": raw_event,
        "failed_at": utc_now_iso(),
    }

    exception.update(extract_partial_ids(raw_event))
    return exception


def build_duplicate_exception(event):
    return {
        "exception_type": "DUPLICATE_EVENT",
        "exception_category": "DUPLICATE_EVENTS",
        "error": "Event with this event_id was already processed",
        "event_id": event["event_id"],
        "order_id": event["order_id"],
        "event_type": event["event_type"],
        "status": "DUPLICATE",
        "failed_at": utc_now_iso(),
    }


def build_too_late_exception(event, current_watermark):
    return {
        "exception_type": "EVENT_TOO_LATE",
        "exception_category": "LIFECYCLE_EXCEPTIONS",
        "error": "Event timestamp is at or before the current watermark",
        "event": event,
        "order_id": event["order_id"],
        "event_id": event["event_id"],
        "event_type": event["event_type"],
        "current_watermark": current_watermark,
        "failed_at": utc_now_iso(),
    }


def build_invalid_transition_exception(
    event,
    previous_status,
    attempted_event,
):
    return {
        "exception_type": "INVALID_LIFECYCLE_TRANSITION",
        "exception_category": "LIFECYCLE_EXCEPTIONS",
        "error": (
            f"Cannot transition from {previous_status} "
            f"to {attempted_event}"
        ),
        "event": event,
        "order_id": event["order_id"],
        "event_id": event["event_id"],
        "event_type": event["event_type"],
        "previous_status": previous_status,
        "attempted_event": attempted_event,
        "failed_at": utc_now_iso(),
    }


def build_row_field_schema(field_index):
    return SerializationSchema(
        get_gateway().jvm.orderlifecycle.RowFieldExtractorSchema(
            field_index
        )
    )


def build_kafka_sink(topic, key_schema, value_schema):
    record_serializer = (
        KafkaRecordSerializationSchema.builder()
        .set_topic(topic)
        .set_key_serialization_schema(key_schema)
        .set_value_serialization_schema(value_schema)
        .build()
    )

    return (
        KafkaSink.builder()
        .set_bootstrap_servers(BOOTSTRAP_SERVERS)
        .set_record_serializer(record_serializer)
        .set_delivery_guarantee(
            DeliveryGuarantee.AT_LEAST_ONCE
        )
        .build()
    )


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
            malformed = build_malformed_exception(
                value,
                str(error),
            )

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

        if self.event_seen.value() is not None:
            duplicate = build_duplicate_exception(event)

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
            exception = build_too_late_exception(
                event,
                current_watermark,
            )

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
                exception = build_invalid_transition_exception(
                    event,
                    previous_status,
                    event_type,
                )

                yield (
                    LIFECYCLE_EXCEPTIONS,
                    json.dumps(exception),
                )

                continue

            self.current_status.update(event_type)

            state_update = build_state_update(
                event,
                previous_status,
                event_type,
            )

            yield STATE_UPDATES, json.dumps(state_update)

            result = build_processed_event(
                event,
                previous_status,
                event_type,
            )

            yield json.dumps(result)


def main():

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.add_jars(SERIALIZER_JAR_PATH.as_uri())

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

    business_events = event_time_events.filter(
        is_business_event
    )

    lifecycle_results = (
        business_events
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

    state_updates = lifecycle_results.get_side_output(
        STATE_UPDATES
    )

    lifecycle_results.print("LIFECYCLE_RESULTS")
    state_updates.print("ORDER_STATE_UPDATES")
    lifecycle_exceptions.print("LIFECYCLE_EXCEPTIONS")
    malformed_events.print("MALFORMED_EVENTS")
    duplicate_events.print("DUPLICATE_EVENTS")

    key_schema = build_row_field_schema(0)
    value_schema = build_row_field_schema(1)

    processed_records = lifecycle_results.map(
        to_kafka_record,
        output_type=KAFKA_RECORD_TYPE,
    )

    state_records = state_updates.map(
        to_kafka_record,
        output_type=KAFKA_RECORD_TYPE,
    )

    exception_records = (
        malformed_events
        .union(duplicate_events, lifecycle_exceptions)
        .map(
            to_kafka_record,
            output_type=KAFKA_RECORD_TYPE,
        )
    )

    processed_records.sink_to(
        build_kafka_sink(
            PROCESSED_TOPIC,
            key_schema,
            value_schema,
        )
    )

    state_records.sink_to(
        build_kafka_sink(
            STATE_TOPIC,
            key_schema,
            value_schema,
        )
    )

    exception_records.sink_to(
        build_kafka_sink(
            EXCEPTIONS_TOPIC,
            key_schema,
            value_schema,
        )
    )

    env.execute("Ecommerce Order Lifecycle Processor")


if __name__ == "__main__":
    main()
