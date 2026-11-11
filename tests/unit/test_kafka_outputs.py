import json
import unittest
import uuid

from pyflink_stubs import load_job_module


class FakeValueState:

    def __init__(self, value=None):
        self._value = value

    def value(self):
        return self._value

    def update(self, value):
        self._value = value


class FakeListState:

    def __init__(self, values):
        self._values = list(values)

    def get(self):
        return iter(self._values)

    def update(self, values):
        self._values = list(values)


class FakeTimerService:

    def __init__(self, watermark):
        self._watermark = watermark

    def current_watermark(self):
        return self._watermark


class FakeContext:

    def __init__(self, watermark):
        self._timer_service = FakeTimerService(watermark)

    def timer_service(self):
        return self._timer_service


def build_event(
    event_type="ORDER_PLACED",
    order_id="ORD-TEST-1",
    event_timestamp="2026-01-01T12:00:00Z",
    source="order_service",
):
    return {
        "schema_version": "1.0",
        "event_id": str(uuid.uuid4()),
        "order_id": order_id,
        "event_type": event_type,
        "event_timestamp": event_timestamp,
        "source": source,
    }


class KafkaRecordTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.job = load_job_module()

    def test_key_prefers_order_id(self):
        key = self.job.select_record_key(
            {
                "order_id": "ORD-1",
                "event_id": "event-1",
            }
        )

        self.assertEqual(key, "ORD-1")

    def test_key_falls_back_to_event_id(self):
        key = self.job.select_record_key(
            {
                "order_id": " ",
                "event_id": "event-1",
            }
        )

        self.assertEqual(key, "event-1")

    def test_key_uses_unknown_when_ids_are_missing(self):
        self.assertEqual(
            self.job.select_record_key({}),
            "unknown",
        )
        self.assertEqual(
            self.job.select_record_key("not-a-record"),
            "unknown",
        )

    def test_to_kafka_record_keeps_payload_and_uses_key(self):
        payload = json.dumps(
            {
                "order_id": "ORD-1",
                "value": 7,
            }
        )

        record = self.job.to_kafka_record(payload)

        self.assertEqual(record.getField(0), "ORD-1")
        self.assertEqual(record.getField(1), payload)

    def test_to_kafka_record_handles_invalid_json(self):
        record = self.job.to_kafka_record("not-json")

        self.assertEqual(record.getField(0), "unknown")
        self.assertEqual(record.getField(1), "not-json")

    def test_state_update_contains_status_and_metadata(self):
        event = build_event()

        state = self.job.build_state_update(
            event,
            None,
            "ORDER_PLACED",
        )

        self.assertEqual(state["order_id"], event["order_id"])
        self.assertEqual(state["current_status"], "ORDER_PLACED")
        self.assertIsNone(state["previous_status"])
        self.assertEqual(
            state["last_event_id"],
            event["event_id"],
        )
        self.assertEqual(
            state["last_event_type"],
            "ORDER_PLACED",
        )
        self.assertEqual(
            state["last_event_timestamp"],
            event["event_timestamp"],
        )
        self.assertEqual(state["last_source"], "order_service")
        self.assertTrue(state["updated_at"].endswith("Z"))

    def test_state_update_tracks_previous_status(self):
        event = build_event("ORDER_CONFIRMED")

        state = self.job.build_state_update(
            event,
            "ORDER_PLACED",
            "ORDER_CONFIRMED",
        )

        self.assertEqual(
            state["previous_status"],
            "ORDER_PLACED",
        )
        self.assertEqual(
            state["current_status"],
            "ORDER_CONFIRMED",
        )

    def test_processed_event_preserves_event_and_adds_status(self):
        event = build_event("ORDER_CONFIRMED")

        processed = self.job.build_processed_event(
            event,
            "ORDER_PLACED",
            "ORDER_CONFIRMED",
        )

        for field, value in event.items():
            self.assertEqual(processed[field], value)

        self.assertEqual(
            processed["processing_status"],
            "ACCEPTED",
        )
        self.assertEqual(
            processed["previous_status"],
            "ORDER_PLACED",
        )
        self.assertEqual(
            processed["current_status"],
            "ORDER_CONFIRMED",
        )
        self.assertTrue(processed["processed_at"].endswith("Z"))

    def test_malformed_exception_includes_available_ids(self):
        raw_event = json.dumps(
            {
                "order_id": "ORD-1",
                "event_id": "event-1",
            }
        )

        exception = self.job.build_malformed_exception(
            raw_event,
            "Missing required fields",
        )

        self.assertEqual(
            exception["exception_type"],
            "MALFORMED_EVENT",
        )
        self.assertEqual(
            exception["exception_category"],
            "MALFORMED_EVENTS",
        )
        self.assertEqual(
            exception["error"],
            "Missing required fields",
        )
        self.assertEqual(exception["raw_event"], raw_event)
        self.assertEqual(exception["order_id"], "ORD-1")
        self.assertEqual(exception["event_id"], "event-1")
        self.assertTrue(exception["failed_at"].endswith("Z"))

    def test_malformed_exception_without_ids(self):
        exception = self.job.build_malformed_exception(
            "not-json",
            "Invalid JSON",
        )

        self.assertNotIn("order_id", exception)
        self.assertNotIn("event_id", exception)

    def test_duplicate_exception_contains_event_details(self):
        event = build_event()

        exception = self.job.build_duplicate_exception(event)

        self.assertEqual(
            exception["exception_type"],
            "DUPLICATE_EVENT",
        )
        self.assertEqual(
            exception["exception_category"],
            "DUPLICATE_EVENTS",
        )
        self.assertEqual(
            exception["event_id"],
            event["event_id"],
        )
        self.assertEqual(
            exception["order_id"],
            event["order_id"],
        )
        self.assertEqual(
            exception["event_type"],
            event["event_type"],
        )
        self.assertEqual(exception["status"], "DUPLICATE")
        self.assertTrue(exception["failed_at"].endswith("Z"))

    def test_too_late_exception_contains_watermark(self):
        event = build_event()

        exception = self.job.build_too_late_exception(
            event,
            1234567890000,
        )

        self.assertEqual(
            exception["exception_type"],
            "EVENT_TOO_LATE",
        )
        self.assertEqual(
            exception["exception_category"],
            "LIFECYCLE_EXCEPTIONS",
        )
        self.assertEqual(
            exception["current_watermark"],
            1234567890000,
        )
        self.assertEqual(exception["order_id"], event["order_id"])
        self.assertEqual(exception["event"], event)

    def test_invalid_transition_exception_contains_context(self):
        event = build_event("ORDER_DELIVERED")

        exception = (
            self.job.build_invalid_transition_exception(
                event,
                "ORDER_PLACED",
                "ORDER_DELIVERED",
            )
        )

        self.assertEqual(
            exception["exception_type"],
            "INVALID_LIFECYCLE_TRANSITION",
        )
        self.assertEqual(
            exception["exception_category"],
            "LIFECYCLE_EXCEPTIONS",
        )
        self.assertEqual(
            exception["previous_status"],
            "ORDER_PLACED",
        )
        self.assertEqual(
            exception["attempted_event"],
            "ORDER_DELIVERED",
        )
        self.assertIn("ORDER_PLACED", exception["error"])
        self.assertEqual(exception["event"], event)


class ExceptionRoutingTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.job = load_job_module()

    def test_malformed_events_are_routed_to_exceptions(self):
        raw_event = json.dumps({"order_id": "ORD-ROUTE-1"})

        output = list(
            self.job.ValidateEvents().process_element(
                raw_event,
                None,
            )
        )

        self.assertEqual(len(output), 1)

        tag, payload = output[0]

        self.assertIs(tag, self.job.MALFORMED_EVENTS)

        record = json.loads(payload)

        self.assertEqual(
            record["exception_category"],
            "MALFORMED_EVENTS",
        )
        self.assertEqual(record["order_id"], "ORD-ROUTE-1")

    def test_duplicate_events_are_routed_to_exceptions(self):
        deduplicate = self.job.DeduplicateEvents()
        deduplicate.event_seen = FakeValueState(True)
        event = build_event()

        output = list(
            deduplicate.process_element(
                json.dumps(event),
                None,
            )
        )

        self.assertEqual(len(output), 1)

        tag, payload = output[0]

        self.assertIs(tag, self.job.DUPLICATE_EVENTS)

        record = json.loads(payload)

        self.assertEqual(
            record["exception_type"],
            "DUPLICATE_EVENT",
        )

    def test_accepted_events_emit_state_and_processed(self):
        lifecycle = self.job.ValidateLifecycle()
        event = build_event()
        timestamp = self.job.parse_event_timestamp(
            event["event_timestamp"]
        )

        lifecycle.current_status = FakeValueState(None)
        lifecycle.pending_events = FakeListState(
            [json.dumps(event)]
        )

        output = list(lifecycle.on_timer(timestamp, None))

        self.assertEqual(len(output), 2)

        state_tag, state_payload = output[0]

        self.assertIs(state_tag, self.job.STATE_UPDATES)

        state = json.loads(state_payload)

        self.assertEqual(state["current_status"], "ORDER_PLACED")
        self.assertIsNone(state["previous_status"])

        processed = json.loads(output[1])

        self.assertEqual(
            processed["processing_status"],
            "ACCEPTED",
        )
        self.assertEqual(
            processed["current_status"],
            "ORDER_PLACED",
        )

    def test_invalid_transitions_are_routed_to_exceptions(self):
        lifecycle = self.job.ValidateLifecycle()
        event = build_event("ORDER_DELIVERED")
        timestamp = self.job.parse_event_timestamp(
            event["event_timestamp"]
        )

        lifecycle.current_status = FakeValueState("ORDER_PLACED")
        lifecycle.pending_events = FakeListState(
            [json.dumps(event)]
        )

        output = list(lifecycle.on_timer(timestamp, None))

        self.assertEqual(len(output), 1)

        tag, payload = output[0]

        self.assertIs(tag, self.job.LIFECYCLE_EXCEPTIONS)

        record = json.loads(payload)

        self.assertEqual(
            record["exception_type"],
            "INVALID_LIFECYCLE_TRANSITION",
        )

    def test_late_events_are_routed_to_exceptions(self):
        lifecycle = self.job.ValidateLifecycle()
        event = build_event(
            event_timestamp="1970-01-01T00:00:00Z"
        )

        output = list(
            lifecycle.process_element(
                json.dumps(event),
                FakeContext(10000),
            )
        )

        self.assertEqual(len(output), 1)

        tag, payload = output[0]

        self.assertIs(tag, self.job.LIFECYCLE_EXCEPTIONS)

        record = json.loads(payload)

        self.assertEqual(
            record["exception_type"],
            "EVENT_TOO_LATE",
        )
        self.assertEqual(record["current_watermark"], 10000)


if __name__ == "__main__":
    unittest.main()
