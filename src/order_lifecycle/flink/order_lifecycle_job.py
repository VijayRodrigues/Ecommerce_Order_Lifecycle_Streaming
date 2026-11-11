
import json
import logging
import os
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
from pyflink.datastream import (
    StreamExecutionEnvironment,
    OutputTag,
    TimeDomain,
)
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

STAGE_DELAY_CONFIG_ENV = "ORDER_LIFECYCLE_PROCESSING_RULES"
STAGE_DELAY_CONFIG_FILENAME = "processing_rules.json"
STAGE_DELAY_CONFIG_KEY = "stage_delay_thresholds_seconds"

TERMINAL_STATUSES = {
    "ORDER_DELIVERED",
    "ORDER_CANCELLED",
}

STAGE_DELAY_ALERT_TYPE = "STAGE_DELAYED"
DELIVERY_DELAY_ALERT_TYPE = "ORDER_DELIVERY_DELAYED"
DELIVERY_DELAY_STATUS = "OUT_FOR_DELIVERY"
DELAY_ALERT_CATEGORY = "DELAY_ALERTS"

EXPECTED_STAGE_DELAYS = {
    "ORDER_PLACED": "ORDER_CONFIRMED",
    "ORDER_CONFIRMED": "ORDER_PACKED",
    "ORDER_PACKED": "ORDER_SHIPPED",
    "ORDER_SHIPPED": "OUT_FOR_DELIVERY",
    "OUT_FOR_DELIVERY": "ORDER_DELIVERED",
}

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

DELAY_ALERTS = OutputTag(
    "delay-alerts",
    Types.STRING(),
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def parse_event_timestamp(timestamp):
    parsed = datetime.fromisoformat(
        timestamp.replace("Z", "+00:00")
    )
    return int(parsed.timestamp() * 1000)


def iso_from_millis(timestamp_ms):
    return (
        datetime.fromtimestamp(
            timestamp_ms / 1000,
            tz=timezone.utc,
        )
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def calculate_duration(previous_timestamp, current_timestamp):
    if previous_timestamp is None or current_timestamp is None:
        return None, None

    seconds = round(
        (
            parse_event_timestamp(current_timestamp)
            - parse_event_timestamp(previous_timestamp)
        )
        / 1000.0,
        3,
    )

    return seconds, round(seconds / 60.0, 3)


def build_delivery_duration(
    order_placed_timestamp,
    order_delivered_timestamp,
):
    seconds, minutes = calculate_duration(
        order_placed_timestamp,
        order_delivered_timestamp,
    )

    return {
        "order_placed_timestamp": order_placed_timestamp,
        "order_delivered_timestamp": order_delivered_timestamp,
        "total_delivery_duration_seconds": seconds,
        "total_delivery_duration_minutes": minutes,
    }


def is_business_event(value):
    return json.loads(value)["event_type"] != WATERMARK_HEARTBEAT


def utc_now_iso():
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def resolve_processing_rules_path(path=None):
    if path is not None:
        return Path(path)

    env_path = os.environ.get(STAGE_DELAY_CONFIG_ENV)

    if env_path:
        return Path(env_path)

    job_path = Path(__file__).resolve()
    candidates = [
        job_path.parent
        / "config"
        / STAGE_DELAY_CONFIG_FILENAME,
    ]

    if len(job_path.parents) > 3:
        candidates.append(
            job_path.parents[3]
            / "config"
            / STAGE_DELAY_CONFIG_FILENAME
        )

    candidates.append(
        Path.cwd() / "config" / STAGE_DELAY_CONFIG_FILENAME
    )

    for candidate in candidates:
        if candidate.is_file():
            return candidate

    raise FileNotFoundError(
        "Could not find processing rules configuration"
    )


def validate_processing_rules(config):
    if not isinstance(config, dict):
        raise ValueError(
            "Processing rules must be a JSON object"
        )

    thresholds = config.get(STAGE_DELAY_CONFIG_KEY)

    if not isinstance(thresholds, dict):
        raise ValueError(
            f"Missing object: {STAGE_DELAY_CONFIG_KEY}"
        )

    expected_keys = {
        f"{from_status}->{to_status}"
        for from_status, to_status in EXPECTED_STAGE_DELAYS.items()
    }

    missing_keys = expected_keys - set(thresholds)
    unexpected_keys = set(thresholds) - expected_keys

    if missing_keys:
        raise ValueError(
            f"Missing stage delay thresholds: {sorted(missing_keys)}"
        )

    if unexpected_keys:
        raise ValueError(
            "Unexpected stage delay thresholds: "
            f"{sorted(unexpected_keys)}"
        )

    normalized = {}

    for from_status, to_status in EXPECTED_STAGE_DELAYS.items():
        key = f"{from_status}->{to_status}"
        threshold = thresholds[key]

        if isinstance(threshold, bool) or not isinstance(
            threshold,
            int,
        ):
            raise ValueError(
                f"Threshold for {key} must be an integer "
                "number of seconds"
            )

        if threshold <= 0:
            raise ValueError(
                f"Threshold for {key} must be greater than zero"
            )

        normalized[from_status] = {
            "next_status": to_status,
            "threshold_seconds": threshold,
        }

    return normalized


def load_processing_rules(path=None):
    config_path = resolve_processing_rules_path(path)

    try:
        with config_path.open(encoding="utf-8") as handle:
            config = json.load(handle)

    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(
            f"Unable to load processing rules from {config_path}: "
            f"{error}"
        ) from error

    return validate_processing_rules(config)


def get_stage_delay_threshold(processing_rules, status):
    rule = processing_rules.get(status)

    if rule is None:
        return None

    return rule["threshold_seconds"]


def get_delay_alert_type(status):
    if status == DELIVERY_DELAY_STATUS:
        return DELIVERY_DELAY_ALERT_TYPE

    return STAGE_DELAY_ALERT_TYPE


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


def build_state_update(
    event,
    previous_status,
    current_status,
    previous_event_timestamp=None,
    order_placed_timestamp=None,
):
    stage_seconds, stage_minutes = calculate_duration(
        previous_event_timestamp,
        event["event_timestamp"],
    )

    state = {
        "order_id": event["order_id"],
        "current_status": current_status,
        "previous_status": previous_status,
        "last_event_id": event["event_id"],
        "last_event_type": event["event_type"],
        "last_event_timestamp": event["event_timestamp"],
        "last_source": event["source"],
        "previous_event_timestamp": previous_event_timestamp,
        "current_event_timestamp": event["event_timestamp"],
        "stage_duration_seconds": stage_seconds,
        "stage_duration_minutes": stage_minutes,
        "updated_at": utc_now_iso(),
    }

    if current_status == "ORDER_DELIVERED":
        state.update(
            build_delivery_duration(
                order_placed_timestamp,
                event["event_timestamp"],
            )
        )

    return state


def build_processed_event(
    event,
    previous_status,
    current_status,
    previous_event_timestamp=None,
    order_placed_timestamp=None,
):
    stage_seconds, stage_minutes = calculate_duration(
        previous_event_timestamp,
        event["event_timestamp"],
    )

    processed = {
        **event,
        "processing_status": "ACCEPTED",
        "previous_status": previous_status,
        "current_status": current_status,
        "previous_event_timestamp": previous_event_timestamp,
        "current_event_timestamp": event["event_timestamp"],
        "stage_duration_seconds": stage_seconds,
        "stage_duration_minutes": stage_minutes,
        "processed_at": utc_now_iso(),
    }

    if current_status == "ORDER_DELIVERED":
        processed.update(
            build_delivery_duration(
                order_placed_timestamp,
                event["event_timestamp"],
            )
        )

    return processed


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


def build_stage_delay_alert(
    order_id,
    current_status,
    stage_started_at,
    elapsed_seconds,
    threshold_seconds,
    alert_type,
    stage_started_processing_at=None,
):
    return {
        "exception_type": alert_type,
        "exception_category": DELAY_ALERT_CATEGORY,
        "alert_type": alert_type,
        "order_id": order_id,
        "current_status": current_status,
        "stage_started_at": stage_started_at,
        "stage_started_processing_at": (
            stage_started_processing_at
        ),
        "elapsed_seconds": round(elapsed_seconds, 3),
        "threshold_seconds": threshold_seconds,
        "alerted_at": utc_now_iso(),
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

    def __init__(self, processing_rules=None):
        self.processing_rules = (
            processing_rules
            if processing_rules is not None
            else load_processing_rules()
        )

    def open(self, runtime_context):
        status_descriptor = ValueStateDescriptor(
            "current_order_status",
            Types.STRING(),
        )

        pending_descriptor = ListStateDescriptor(
            "pending_order_events",
            Types.STRING(),
        )

        last_event_descriptor = ValueStateDescriptor(
            "last_accepted_event_timestamp",
            Types.STRING(),
        )

        placed_descriptor = ValueStateDescriptor(
            "order_placed_timestamp",
            Types.STRING(),
        )

        stage_started_descriptor = ValueStateDescriptor(
            "stage_started_processing_ms",
            Types.LONG(),
        )

        alert_deadline_descriptor = ValueStateDescriptor(
            "stage_alert_deadline_ms",
            Types.LONG(),
        )

        alert_sent_descriptor = ValueStateDescriptor(
            "stage_alert_sent",
            Types.BOOLEAN(),
        )

        self.current_status = runtime_context.get_state(
            status_descriptor
        )

        self.pending_events = runtime_context.get_list_state(
            pending_descriptor
        )

        self.last_accepted_event_timestamp = (
            runtime_context.get_state(last_event_descriptor)
        )

        self.order_placed_timestamp = runtime_context.get_state(
            placed_descriptor
        )

        self.stage_started_processing_ms = (
            runtime_context.get_state(stage_started_descriptor)
        )

        self.stage_alert_deadline_ms = runtime_context.get_state(
            alert_deadline_descriptor
        )

        self.stage_alert_sent = runtime_context.get_state(
            alert_sent_descriptor
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
        if ctx.time_domain() == TimeDomain.EVENT_TIME:
            yield from self._process_due_events(
                timestamp,
                ctx,
            )
            return

        if ctx.time_domain() == TimeDomain.PROCESSING_TIME:
            yield from self._handle_delay_alert(
                timestamp,
                ctx,
            )

    def _process_due_events(self, timestamp, ctx):
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

            yield from self._accept_event(event, ctx)

    def _accept_event(self, event, ctx):
        previous_status = self.current_status.value()
        previous_event_timestamp = (
            self.last_accepted_event_timestamp.value()
        )
        order_placed_timestamp = (
            self.order_placed_timestamp.value()
        )
        event_type = event["event_type"]

        self._cancel_delay_timer(ctx)

        if event_type == "ORDER_PLACED":
            order_placed_timestamp = event["event_timestamp"]
            self.order_placed_timestamp.update(
                order_placed_timestamp
            )

        self.current_status.update(event_type)
        self.last_accepted_event_timestamp.update(
            event["event_timestamp"]
        )

        state_update = build_state_update(
            event,
            previous_status,
            event_type,
            previous_event_timestamp=previous_event_timestamp,
            order_placed_timestamp=order_placed_timestamp,
        )

        yield STATE_UPDATES, json.dumps(state_update)

        result = build_processed_event(
            event,
            previous_status,
            event_type,
            previous_event_timestamp=previous_event_timestamp,
            order_placed_timestamp=order_placed_timestamp,
        )

        yield json.dumps(result)

        if event_type not in TERMINAL_STATUSES:
            self._schedule_delay_timer(event_type, ctx)

    def _schedule_delay_timer(self, status, ctx):
        threshold = get_stage_delay_threshold(
            self.processing_rules,
            status,
        )

        if threshold is None:
            return

        processing_now = (
            ctx.timer_service().current_processing_time()
        )
        deadline = processing_now + (threshold * 1000)

        self.stage_started_processing_ms.update(
            processing_now
        )
        self.stage_alert_deadline_ms.update(deadline)
        self.stage_alert_sent.update(False)

        ctx.timer_service().register_processing_time_timer(
            deadline
        )

    def _cancel_delay_timer(self, ctx):
        deadline = self.stage_alert_deadline_ms.value()

        if deadline is not None:
            ctx.timer_service().delete_processing_time_timer(
                deadline
            )

        self.stage_alert_deadline_ms.clear()
        self.stage_alert_sent.clear()
        self.stage_started_processing_ms.clear()

    def _handle_delay_alert(self, timestamp, ctx):
        if self.stage_alert_deadline_ms.value() != timestamp:
            return

        status = self.current_status.value()

        self.stage_alert_deadline_ms.clear()

        if status is None or status in TERMINAL_STATUSES:
            self.stage_alert_sent.clear()
            self.stage_started_processing_ms.clear()
            return

        if self.stage_alert_sent.value() is True:
            return

        threshold = get_stage_delay_threshold(
            self.processing_rules,
            status,
        )

        if threshold is None:
            return

        stage_started_at = (
            self.last_accepted_event_timestamp.value()
        )
        stage_started_processing_ms = (
            self.stage_started_processing_ms.value()
        )

        if (
            stage_started_at is None
            or stage_started_processing_ms is None
        ):
            return

        if self._stage_completed_within_event_time(
            status,
            stage_started_at,
            threshold,
        ):
            self.stage_alert_sent.update(True)
            self.stage_started_processing_ms.clear()
            return

        processing_now = (
            ctx.timer_service().current_processing_time()
        )
        elapsed_seconds = max(
            0.0,
            (
                processing_now
                - stage_started_processing_ms
            )
            / 1000.0,
        )

        alert = build_stage_delay_alert(
            order_id=ctx.get_current_key(),
            current_status=status,
            stage_started_at=stage_started_at,
            stage_started_processing_at=iso_from_millis(
                stage_started_processing_ms
            ),
            elapsed_seconds=elapsed_seconds,
            threshold_seconds=threshold,
            alert_type=get_delay_alert_type(status),
        )

        self.stage_alert_sent.update(True)
        self.stage_started_processing_ms.clear()

        yield DELAY_ALERTS, json.dumps(alert)

    def _stage_completed_within_event_time(
        self,
        status,
        stage_started_at,
        threshold_seconds,
    ):
        stage_started_ms = parse_event_timestamp(
            stage_started_at
        )
        stage_limit_ms = (
            stage_started_ms + (threshold_seconds * 1000)
        )
        allowed = ALLOWED_TRANSITIONS.get(status, set())

        for value in self.pending_events.get():
            event = json.loads(value)
            event_type = event["event_type"]

            if event_type not in allowed:
                continue

            if event_type == "ORDER_CANCELLED":
                return True

            if (
                parse_event_timestamp(
                    event["event_timestamp"]
                )
                <= stage_limit_ms
            ):
                return True

        return False


def main():

    processing_rules = load_processing_rules()

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
            ValidateLifecycle(processing_rules),
            output_type=Types.STRING(),
        )
    )

    lifecycle_exceptions = lifecycle_results.get_side_output(
        LIFECYCLE_EXCEPTIONS
    )

    state_updates = lifecycle_results.get_side_output(
        STATE_UPDATES
    )

    delay_alerts = lifecycle_results.get_side_output(
        DELAY_ALERTS
    )

    lifecycle_results.print("LIFECYCLE_RESULTS")
    state_updates.print("ORDER_STATE_UPDATES")
    lifecycle_exceptions.print("LIFECYCLE_EXCEPTIONS")
    delay_alerts.print("DELAY_ALERTS")
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
        .union(
            duplicate_events,
            lifecycle_exceptions,
            delay_alerts,
        )
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
