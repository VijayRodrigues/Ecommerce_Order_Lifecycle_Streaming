import contextlib
import io
import json
import os
import unittest

from types import SimpleNamespace
from unittest.mock import patch

from kafka import TopicPartition
from kafka.errors import KafkaError

from order_lifecycle.consumers.common import (
    DEFAULT_EXCEPTIONS_TOPIC,
    DEFAULT_PROCESSED_TOPIC,
    DEFAULT_STATE_TOPIC,
    bootstrap_servers_default,
    build_arg_parser,
    create_consumer,
    create_manual_consumer,
    decode_key,
    display_value,
    format_exception_record,
    format_processed_record,
    format_record_header,
    format_state_record,
    parse_record_value,
    topic_from_env,
    validate_args,
)


class ParsingTests(unittest.TestCase):

    def test_parses_json_bytes(self):
        value = parse_record_value(
            json.dumps({"order_id": "ORD-1"}).encode("utf-8")
        )

        self.assertEqual(value["order_id"], "ORD-1")

    def test_parses_json_string(self):
        value = parse_record_value('{"order_id": "ORD-2"}')

        self.assertEqual(value["order_id"], "ORD-2")

    def test_rejects_invalid_json(self):
        with self.assertRaisesRegex(ValueError, "Invalid JSON"):
            parse_record_value(b"not-json")

    def test_rejects_non_object_json(self):
        with self.assertRaisesRegex(ValueError, "not a JSON object"):
            parse_record_value(b"[]")

    def test_rejects_non_json_value_type(self):
        with self.assertRaisesRegex(ValueError, "Unsupported"):
            parse_record_value(123)

    def test_decodes_key_and_handles_missing_key(self):
        self.assertEqual(decode_key(b"ORD-1"), "ORD-1")
        self.assertEqual(decode_key(None), "-")
        self.assertEqual(decode_key("ORD-2"), "ORD-2")


class FormattingTests(unittest.TestCase):

    def test_state_record_displays_status_timestamps_and_durations(self):
        text = format_state_record(
            {
                "order_id": "ORD-1",
                "current_status": "ORDER_DELIVERED",
                "previous_status": "OUT_FOR_DELIVERY",
                "last_event_type": "ORDER_DELIVERED",
                "last_event_timestamp": "2026-01-01T10:50:00Z",
                "previous_event_timestamp": "2026-01-01T10:40:00Z",
                "current_event_timestamp": "2026-01-01T10:50:00Z",
                "stage_duration_seconds": 600,
                "stage_duration_minutes": 10.0,
                "order_placed_timestamp": "2026-01-01T10:00:00Z",
                "order_delivered_timestamp": "2026-01-01T10:50:00Z",
                "total_delivery_duration_seconds": 3000,
                "total_delivery_duration_minutes": 50.0,
                "updated_at": "2026-01-01T10:50:01Z",
            }
        )

        self.assertIn("ORDER STATE", text)
        self.assertIn("order_id: ORD-1", text)
        self.assertIn("current_status: ORDER_DELIVERED", text)
        self.assertIn("previous_status: OUT_FOR_DELIVERY", text)
        self.assertIn("stage_duration_seconds: 600", text)
        self.assertIn("total_delivery_duration_seconds: 3000", text)

    def test_state_record_uses_placeholder_for_missing_fields(self):
        text = format_state_record({"order_id": "ORD-1"})

        self.assertIn("previous_status: -", text)
        self.assertIn("total_delivery_duration_seconds: -", text)

    def test_processed_record_displays_status_and_duration(self):
        text = format_processed_record(
            {
                "order_id": "ORD-2",
                "event_id": "event-2",
                "event_type": "ORDER_CONFIRMED",
                "event_timestamp": "2026-01-01T10:10:00Z",
                "previous_status": "ORDER_PLACED",
                "current_status": "ORDER_CONFIRMED",
                "stage_duration_seconds": 600,
                "stage_duration_minutes": 10.0,
                "source": "order_service",
                "processing_status": "ACCEPTED",
                "processed_at": "2026-01-01T10:10:01Z",
            }
        )

        self.assertIn("PROCESSED EVENT", text)
        self.assertIn("event_type: ORDER_CONFIRMED", text)
        self.assertIn("previous_status: ORDER_PLACED", text)
        self.assertIn("current_status: ORDER_CONFIRMED", text)
        self.assertIn("stage_duration_seconds: 600", text)
        self.assertIn("processing_status: ACCEPTED", text)

    def test_exception_record_displays_alert_details(self):
        text = format_exception_record(
            {
                "exception_type": "STAGE_DELAYED",
                "alert_type": "STAGE_DELAYED",
                "exception_category": "DELAY_ALERTS",
                "order_id": "ORD-3",
                "current_status": "ORDER_PLACED",
                "elapsed_seconds": 185.5,
                "threshold_seconds": 180,
                "alerted_at": "2026-01-01T10:03:05Z",
            }
        )

        self.assertIn("EXCEPTION / ALERT", text)
        self.assertIn("exception_type: STAGE_DELAYED", text)
        self.assertIn("alert_type: STAGE_DELAYED", text)
        self.assertIn("exception_category: DELAY_ALERTS", text)
        self.assertIn("order_id: ORD-3", text)
        self.assertIn("elapsed_seconds: 185.5", text)

    def test_exception_record_displays_malformed_details(self):
        text = format_exception_record(
            {
                "exception_type": "MALFORMED_EVENT",
                "exception_category": "MALFORMED_EVENTS",
                "error": "Missing required fields",
            }
        )

        self.assertIn("exception_type: MALFORMED_EVENT", text)
        self.assertIn("alert_type: -", text)
        self.assertIn("order_id: -", text)
        self.assertIn("error: Missing required fields", text)

    def test_record_header_includes_topic_partition_and_key(self):
        record = SimpleNamespace(partition=1, offset=42, key=b"ORD-4")

        self.assertEqual(
            format_record_header("state-topic", record),
            "[state-topic] partition=1 offset=42 key=ORD-4",
        )

    def test_display_value(self):
        self.assertEqual(display_value(None), "-")
        self.assertEqual(display_value(""), "-")
        self.assertEqual(display_value(0), "0")


class ConsumerConstructionTests(unittest.TestCase):

    @patch("order_lifecycle.consumers.common.KafkaConsumer")
    def test_manual_consumer_assigns_all_partitions(self, kafka_consumer):
        consumer = kafka_consumer.return_value
        consumer.partitions_for_topic.return_value = {1, 0}

        result = create_manual_consumer(
            "orders.state",
            "broker:9092",
            from_beginning=True,
        )

        self.assertIs(result, consumer)
        consumer.assign.assert_called_once_with(
            [
                TopicPartition("orders.state", 0),
                TopicPartition("orders.state", 1),
            ]
        )
        consumer.seek_to_beginning.assert_called_once()

    @patch("order_lifecycle.consumers.common.KafkaConsumer")
    def test_manual_consumer_rejects_missing_topic(self, kafka_consumer):
        kafka_consumer.return_value.partitions_for_topic.return_value = None

        with self.assertRaisesRegex(KafkaError, "no partitions"):
            create_manual_consumer(
                "missing.state",
                "broker:9092",
                from_beginning=False,
            )

    @patch("order_lifecycle.consumers.common.create_manual_consumer")
    @patch("order_lifecycle.consumers.common.create_group_consumer")
    def test_create_consumer_falls_back_when_group_is_unavailable(
        self,
        create_group_consumer,
        create_manual_consumer,
    ):
        manual_consumer = object()
        create_group_consumer.return_value = None
        create_manual_consumer.return_value = manual_consumer

        with contextlib.redirect_stderr(io.StringIO()):
            result = create_consumer(
                "orders.state",
                "state-group",
                "broker:9092",
                from_beginning=True,
            )

        self.assertIs(result, manual_consumer)
        create_manual_consumer.assert_called_once_with(
            "orders.state",
            "broker:9092",
            True,
        )

    @patch("order_lifecycle.consumers.common.create_manual_consumer")
    @patch("order_lifecycle.consumers.common.create_group_consumer")
    def test_create_consumer_uses_group_when_available(
        self,
        create_group_consumer,
        create_manual_consumer,
    ):
        group_consumer = object()
        create_group_consumer.return_value = group_consumer

        result = create_consumer(
            "orders.state",
            "state-group",
            "broker:9092",
            from_beginning=False,
        )

        self.assertIs(result, group_consumer)
        create_manual_consumer.assert_not_called()


class ConfigurationTests(unittest.TestCase):

    def test_bootstrap_servers_can_be_overridden(self):
        with patch.dict(
            os.environ,
            {"KAFKA_BOOTSTRAP_SERVERS": "broker:9092"},
        ):
            self.assertEqual(
                bootstrap_servers_default(),
                "broker:9092",
            )

    def test_topic_can_be_overridden(self):
        with patch.dict(
            os.environ,
            {"KAFKA_STATE_TOPIC": "custom.state"},
        ):
            self.assertEqual(
                topic_from_env(
                    "KAFKA_STATE_TOPIC",
                    DEFAULT_STATE_TOPIC,
                ),
                "custom.state",
            )

    def test_default_topics_match_project_topics(self):
        self.assertEqual(
            DEFAULT_STATE_TOPIC,
            "ecommerce.order-lifecycle.state",
        )
        self.assertEqual(
            DEFAULT_PROCESSED_TOPIC,
            "ecommerce.order-lifecycle.processed",
        )
        self.assertEqual(
            DEFAULT_EXCEPTIONS_TOPIC,
            "ecommerce.order-lifecycle.exceptions",
        )

    def test_consumer_groups_are_separate(self):
        from order_lifecycle.consumers.exceptions_consumer import (
            DEFAULT_GROUP_ID as EXCEPTIONS_GROUP,
        )
        from order_lifecycle.consumers.processed_consumer import (
            DEFAULT_GROUP_ID as PROCESSED_GROUP,
        )
        from order_lifecycle.consumers.state_consumer import (
            DEFAULT_GROUP_ID as STATE_GROUP,
        )

        self.assertEqual(len({STATE_GROUP, PROCESSED_GROUP, EXCEPTIONS_GROUP}), 3)

    def test_parser_accepts_consumer_flags(self):
        parser = build_arg_parser("test", "test-group")
        args = parser.parse_args(
            [
                "--from-beginning",
                "--max-messages",
                "2",
                "--idle-timeout",
                "3",
            ]
        )

        self.assertTrue(args.from_beginning)
        self.assertEqual(args.max_messages, 2)
        self.assertEqual(args.idle_timeout, 3)
        self.assertEqual(args.group_id, "test-group")

    def test_validation_rejects_non_positive_limits(self):
        parser = build_arg_parser("test", "test-group")

        with self.assertRaises(SystemExit):
            validate_args(
                parser,
                parser.parse_args(["--max-messages", "0"]),
            )

        with self.assertRaises(SystemExit):
            validate_args(
                parser,
                parser.parse_args(["--idle-timeout", "0"]),
            )


if __name__ == "__main__":
    unittest.main()
