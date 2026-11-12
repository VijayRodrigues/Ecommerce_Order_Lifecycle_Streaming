"""Shared helpers for the independent Kafka consumers."""

import argparse
import json
import os
import sys
import time

from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaError


DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
RECONNECT_DELAY_SECONDS = 5.0
GROUP_ASSIGNMENT_TIMEOUT_SECONDS = 5.0

DEFAULT_STATE_TOPIC = "ecommerce.order-lifecycle.state"
DEFAULT_PROCESSED_TOPIC = "ecommerce.order-lifecycle.processed"
DEFAULT_EXCEPTIONS_TOPIC = "ecommerce.order-lifecycle.exceptions"


def bootstrap_servers_default():
    return os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        DEFAULT_BOOTSTRAP_SERVERS,
    )


def topic_from_env(variable, default):
    return os.getenv(variable, default)


def build_arg_parser(description, default_group_id):
    parser = argparse.ArgumentParser(description=description)

    parser.add_argument(
        "--bootstrap-servers",
        default=bootstrap_servers_default(),
        help=(
            "Kafka bootstrap servers. Defaults to "
            "KAFKA_BOOTSTRAP_SERVERS or localhost:9092."
        ),
    )

    parser.add_argument(
        "--group-id",
        default=default_group_id,
        help="Consumer group ID for this independent consumer.",
    )

    parser.add_argument(
        "--from-beginning",
        action="store_true",
        help=(
            "Read retained records from the beginning and then keep "
            "following new records."
        ),
    )

    parser.add_argument(
        "--max-messages",
        type=int,
        default=None,
        help="Exit after this many valid records.",
    )

    parser.add_argument(
        "--idle-timeout",
        type=float,
        default=None,
        help="Exit after this many seconds without records.",
    )

    return parser


def validate_args(parser, args):
    if args.max_messages is not None and args.max_messages < 1:
        parser.error("--max-messages must be greater than zero")

    if args.idle_timeout is not None and args.idle_timeout <= 0:
        parser.error("--idle-timeout must be greater than zero")


def parse_record_value(raw_value):
    if isinstance(raw_value, dict):
        return raw_value

    if isinstance(raw_value, bytes):
        raw_value = raw_value.decode("utf-8")

    if not isinstance(raw_value, str):
        raise ValueError(
            f"Unsupported Kafka value type: {type(raw_value).__name__}"
        )

    try:
        value = json.loads(raw_value)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON: {exc}") from exc

    if not isinstance(value, dict):
        raise ValueError("Kafka message value is not a JSON object")

    return value


def decode_key(key):
    if key is None:
        return "-"

    if isinstance(key, bytes):
        return key.decode("utf-8", errors="replace")

    return str(key)


def display_value(value):
    if value is None or value == "":
        return "-"
    return str(value)


def format_record_header(topic, record):
    return (
        f"[{topic}] partition={record.partition} "
        f"offset={record.offset} key={decode_key(record.key)}"
    )


def format_state_record(record):
    return "\n".join(
        [
            "ORDER STATE",
            f"  order_id: {display_value(record.get('order_id'))}",
            f"  current_status: {display_value(record.get('current_status'))}",
            f"  previous_status: {display_value(record.get('previous_status'))}",
            "  last_event_type: "
            f"{display_value(record.get('last_event_type'))}",
            "  last_event_timestamp: "
            f"{display_value(record.get('last_event_timestamp'))}",
            "  previous_event_timestamp: "
            f"{display_value(record.get('previous_event_timestamp'))}",
            "  current_event_timestamp: "
            f"{display_value(record.get('current_event_timestamp'))}",
            "  stage_duration_seconds: "
            f"{display_value(record.get('stage_duration_seconds'))}",
            "  stage_duration_minutes: "
            f"{display_value(record.get('stage_duration_minutes'))}",
            "  order_placed_timestamp: "
            f"{display_value(record.get('order_placed_timestamp'))}",
            "  order_delivered_timestamp: "
            f"{display_value(record.get('order_delivered_timestamp'))}",
            "  total_delivery_duration_seconds: "
            f"{display_value(record.get('total_delivery_duration_seconds'))}",
            "  total_delivery_duration_minutes: "
            f"{display_value(record.get('total_delivery_duration_minutes'))}",
            f"  updated_at: {display_value(record.get('updated_at'))}",
        ]
    )


def format_processed_record(record):
    return "\n".join(
        [
            "PROCESSED EVENT",
            f"  order_id: {display_value(record.get('order_id'))}",
            f"  event_id: {display_value(record.get('event_id'))}",
            f"  event_type: {display_value(record.get('event_type'))}",
            f"  event_timestamp: {display_value(record.get('event_timestamp'))}",
            f"  previous_status: {display_value(record.get('previous_status'))}",
            f"  current_status: {display_value(record.get('current_status'))}",
            "  stage_duration_seconds: "
            f"{display_value(record.get('stage_duration_seconds'))}",
            "  stage_duration_minutes: "
            f"{display_value(record.get('stage_duration_minutes'))}",
            "  total_delivery_duration_seconds: "
            f"{display_value(record.get('total_delivery_duration_seconds'))}",
            "  total_delivery_duration_minutes: "
            f"{display_value(record.get('total_delivery_duration_minutes'))}",
            f"  source: {display_value(record.get('source'))}",
            "  processing_status: "
            f"{display_value(record.get('processing_status'))}",
            f"  processed_at: {display_value(record.get('processed_at'))}",
        ]
    )


def format_exception_record(record):
    return "\n".join(
        [
            "EXCEPTION / ALERT",
            "  exception_type: "
            f"{display_value(record.get('exception_type'))}",
            f"  alert_type: {display_value(record.get('alert_type'))}",
            "  exception_category: "
            f"{display_value(record.get('exception_category'))}",
            f"  order_id: {display_value(record.get('order_id'))}",
            f"  event_id: {display_value(record.get('event_id'))}",
            f"  event_type: {display_value(record.get('event_type'))}",
            f"  current_status: {display_value(record.get('current_status'))}",
            "  previous_status: "
            f"{display_value(record.get('previous_status'))}",
            "  attempted_event: "
            f"{display_value(record.get('attempted_event'))}",
            f"  error: {display_value(record.get('error'))}",
            "  current_watermark: "
            f"{display_value(record.get('current_watermark'))}",
            f"  elapsed_seconds: {display_value(record.get('elapsed_seconds'))}",
            "  threshold_seconds: "
            f"{display_value(record.get('threshold_seconds'))}",
            f"  failed_at: {display_value(record.get('failed_at'))}",
            f"  alerted_at: {display_value(record.get('alerted_at'))}",
        ]
    )


def create_manual_consumer(topic, bootstrap_servers, from_beginning):
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset=("earliest" if from_beginning else "latest"),
        enable_auto_commit=False,
    )

    partitions = consumer.partitions_for_topic(topic)
    if not partitions:
        consumer.close()
        raise KafkaError(f"Topic does not exist or has no partitions: {topic}")

    assignments = [
        TopicPartition(topic, partition)
        for partition in sorted(partitions)
    ]
    consumer.assign(assignments)

    if from_beginning:
        consumer.seek_to_beginning(*assignments)

    return consumer


def create_group_consumer(topic, group_id, bootstrap_servers, from_beginning):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        auto_offset_reset=("earliest" if from_beginning else "latest"),
        enable_auto_commit=True,
    )

    deadline = time.monotonic() + GROUP_ASSIGNMENT_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        consumer.poll(timeout_ms=500)
        partitions = consumer.assignment()
        if partitions:
            if from_beginning:
                consumer.seek_to_beginning(*partitions)
            return consumer

    consumer.close()
    return None


def create_consumer(topic, group_id, bootstrap_servers, from_beginning):
    try:
        consumer = create_group_consumer(
            topic,
            group_id,
            bootstrap_servers,
            from_beginning,
        )
        if consumer is not None:
            return consumer
    except (KafkaError, OSError):
        pass

    print(
        f"Consumer group coordination is unavailable for {topic}; "
        "falling back to manual partition assignment. "
        "Offsets will not be committed.",
        file=sys.stderr,
        flush=True,
    )
    return create_manual_consumer(topic, bootstrap_servers, from_beginning)


def run_consumer(
    topic,
    group_id,
    bootstrap_servers,
    format_record,
    from_beginning=False,
    max_messages=None,
    idle_timeout=None,
):
    consumed = 0

    while True:
        consumer = None

        try:
            consumer = create_consumer(
                topic,
                group_id,
                bootstrap_servers,
                from_beginning,
            )

            last_message_at = time.monotonic()

            while True:
                batches = consumer.poll(timeout_ms=1000)

                if batches:
                    last_message_at = time.monotonic()

                for records in batches.values():
                    for record in records:
                        try:
                            value = parse_record_value(record.value)
                        except ValueError as exc:
                            print(
                                f"Skipping malformed message in {topic}: "
                                f"{exc}",
                                file=sys.stderr,
                            )
                            continue

                        print(format_record_header(topic, record))
                        print(format_record(value), flush=True)

                        consumed += 1
                        if max_messages is not None and consumed >= max_messages:
                            return 0

                if (
                    idle_timeout is not None
                    and time.monotonic() - last_message_at >= idle_timeout
                ):
                    return 0

        except KeyboardInterrupt:
            return 0
        except (KafkaError, OSError) as exc:
            print(
                f"Kafka consumer error for {topic}: {exc}. "
                f"Retrying in {RECONNECT_DELAY_SECONDS:.0f} seconds.",
                file=sys.stderr,
            )
            time.sleep(RECONNECT_DELAY_SECONDS)
        finally:
            if consumer is not None:
                consumer.close()
