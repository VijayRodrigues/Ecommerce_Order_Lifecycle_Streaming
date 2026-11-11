import json
import os
import tempfile
import unittest
import uuid

from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from pyflink_stubs import load_job_module


BASE_TIME = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

STAGE_THRESHOLD_SECONDS = 5

TEST_RULES = {
    "stage_delay_thresholds_seconds": {
        "ORDER_PLACED->ORDER_CONFIRMED": STAGE_THRESHOLD_SECONDS,
        "ORDER_CONFIRMED->ORDER_PACKED": STAGE_THRESHOLD_SECONDS,
        "ORDER_PACKED->ORDER_SHIPPED": STAGE_THRESHOLD_SECONDS,
        "ORDER_SHIPPED->OUT_FOR_DELIVERY": STAGE_THRESHOLD_SECONDS,
        "OUT_FOR_DELIVERY->ORDER_DELIVERED": STAGE_THRESHOLD_SECONDS,
    }
}

STAGE_DURATIONS = [
    ("ORDER_PLACED", "ORDER_CONFIRMED", 600),
    ("ORDER_CONFIRMED", "ORDER_PACKED", 1800),
    ("ORDER_PACKED", "ORDER_SHIPPED", 3600),
    ("ORDER_SHIPPED", "OUT_FOR_DELIVERY", 7200),
    ("OUT_FOR_DELIVERY", "ORDER_DELIVERED", 900),
]


class FakeValueState:

    def __init__(self, value=None):
        self._value = value

    def value(self):
        return self._value

    def update(self, value):
        self._value = value

    def clear(self):
        self._value = None


class FakeListState:

    def __init__(self, values=None):
        self._values = list(values or [])

    def get(self):
        return iter(self._values)

    def add(self, value):
        self._values.append(value)

    def update(self, values):
        self._values = list(values)


class FakeTimerService:

    def __init__(
        self,
        watermark=0,
        processing_time=1000000,
    ):
        self.watermark = watermark
        self.processing_time = processing_time
        self.event_timers = []
        self.processing_timers = []
        self.deleted_processing_timers = []

    def current_watermark(self):
        return self.watermark

    def current_processing_time(self):
        return self.processing_time

    def register_event_time_timer(self, timestamp):
        self.event_timers.append(timestamp)

    def register_processing_time_timer(self, timestamp):
        self.processing_timers.append(timestamp)

    def delete_processing_time_timer(self, timestamp):
        self.deleted_processing_timers.append(timestamp)


class FakeContext:

    def __init__(
        self,
        watermark=0,
        processing_time=1000000,
        current_key="ORD-TEST-1",
        time_domain="EVENT_TIME",
    ):
        self.timers = FakeTimerService(
            watermark,
            processing_time,
        )
        self.current_key = current_key
        self.domain = time_domain

    def timer_service(self):
        return self.timers

    def get_current_key(self):
        return self.current_key

    def time_domain(self):
        return self.domain


def iso(timestamp):
    return timestamp.isoformat().replace("+00:00", "Z")


def build_event(
    event_type,
    event_timestamp,
    order_id="ORD-TEST-1",
    source="order_service",
):
    return {
        "schema_version": "1.0",
        "event_id": str(uuid.uuid4()),
        "order_id": order_id,
        "event_type": event_type,
        "event_timestamp": iso(event_timestamp),
        "source": source,
    }


def make_lifecycle(
    job,
    rules,
    status=None,
    pending=None,
    last_timestamp=None,
    placed_timestamp=None,
    event_timers=None,
):
    lifecycle = job.ValidateLifecycle(rules)
    lifecycle.current_status = FakeValueState(status)
    lifecycle.pending_events = FakeListState(pending)
    lifecycle.last_accepted_event_timestamp = FakeValueState(
        last_timestamp
    )
    lifecycle.order_placed_timestamp = FakeValueState(
        placed_timestamp
    )
    lifecycle.stage_started_processing_ms = FakeValueState(None)
    lifecycle.stage_alert_deadline_ms = FakeValueState(None)
    lifecycle.stage_alert_sent = FakeValueState(None)
    return lifecycle


def accept_placed(job, rules, processing_time=1000000):
    placed = build_event("ORDER_PLACED", BASE_TIME)
    placed_ms = job.parse_event_timestamp(
        placed["event_timestamp"]
    )

    lifecycle = make_lifecycle(
        job,
        rules,
        pending=[json.dumps(placed)],
    )
    ctx = FakeContext(
        watermark=placed_ms - 1,
        processing_time=processing_time,
    )

    output = list(lifecycle.on_timer(placed_ms, ctx))
    return lifecycle, ctx, output, placed


class DurationCalculationTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.job = load_job_module()

    def test_first_placed_event_has_null_stage_duration(self):
        event = build_event("ORDER_PLACED", BASE_TIME)

        processed = self.job.build_processed_event(
            event,
            None,
            "ORDER_PLACED",
            previous_event_timestamp=None,
        )
        state = self.job.build_state_update(
            event,
            None,
            "ORDER_PLACED",
            previous_event_timestamp=None,
        )

        for record in (processed, state):
            self.assertIsNone(
                record["previous_event_timestamp"]
            )
            self.assertEqual(
                record["current_event_timestamp"],
                event["event_timestamp"],
            )
            self.assertIsNone(
                record["stage_duration_seconds"]
            )
            self.assertIsNone(
                record["stage_duration_minutes"]
            )

    def test_every_stage_duration_uses_event_timestamp(self):
        previous = BASE_TIME

        for previous_status, current_status, seconds in (
            STAGE_DURATIONS
        ):
            with self.subTest(
                previous_status=previous_status,
                current_status=current_status,
            ):
                current = previous + timedelta(seconds=seconds)
                event = build_event(current_status, current)

                processed = self.job.build_processed_event(
                    event,
                    previous_status,
                    current_status,
                    previous_event_timestamp=iso(previous),
                )
                state = self.job.build_state_update(
                    event,
                    previous_status,
                    current_status,
                    previous_event_timestamp=iso(previous),
                )

                expected_minutes = round(seconds / 60.0, 3)

                for record in (processed, state):
                    self.assertEqual(
                        record["previous_event_timestamp"],
                        iso(previous),
                    )
                    self.assertEqual(
                        record["current_event_timestamp"],
                        iso(current),
                    )
                    self.assertEqual(
                        record["stage_duration_seconds"],
                        seconds,
                    )
                    self.assertEqual(
                        record["stage_duration_minutes"],
                        expected_minutes,
                    )

                previous = current

    def test_total_delivery_duration_on_delivered_records(self):
        placed_at = BASE_TIME
        delivered_at = placed_at + timedelta(minutes=50)
        event = build_event("ORDER_DELIVERED", delivered_at)

        processed = self.job.build_processed_event(
            event,
            "OUT_FOR_DELIVERY",
            "ORDER_DELIVERED",
            previous_event_timestamp=iso(
                delivered_at - timedelta(minutes=10)
            ),
            order_placed_timestamp=iso(placed_at),
        )
        state = self.job.build_state_update(
            event,
            "OUT_FOR_DELIVERY",
            "ORDER_DELIVERED",
            previous_event_timestamp=iso(
                delivered_at - timedelta(minutes=10)
            ),
            order_placed_timestamp=iso(placed_at),
        )

        for record in (processed, state):
            self.assertEqual(
                record["order_placed_timestamp"],
                iso(placed_at),
            )
            self.assertEqual(
                record["order_delivered_timestamp"],
                iso(delivered_at),
            )
            self.assertEqual(
                record["total_delivery_duration_seconds"],
                3000,
            )
            self.assertEqual(
                record["total_delivery_duration_minutes"],
                50,
            )
            self.assertEqual(
                record["stage_duration_seconds"],
                600,
            )

    def test_cancelled_and_incomplete_orders_have_no_total_duration(self):
        cancelled = build_event(
            "ORDER_CANCELLED",
            BASE_TIME + timedelta(minutes=5),
        )
        shipped = build_event(
            "ORDER_SHIPPED",
            BASE_TIME + timedelta(minutes=15),
        )

        cancelled_record = self.job.build_processed_event(
            cancelled,
            "ORDER_PLACED",
            "ORDER_CANCELLED",
            previous_event_timestamp=iso(BASE_TIME),
            order_placed_timestamp=iso(BASE_TIME),
        )
        shipped_record = self.job.build_state_update(
            shipped,
            "ORDER_PACKED",
            "ORDER_SHIPPED",
            previous_event_timestamp=iso(BASE_TIME),
            order_placed_timestamp=iso(BASE_TIME),
        )

        self.assertNotIn(
            "total_delivery_duration_seconds",
            cancelled_record,
        )
        self.assertNotIn(
            "total_delivery_duration_seconds",
            shipped_record,
        )


class ConfigLoadingTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.job = load_job_module()

    def write_config(self, directory, config):
        path = Path(directory) / "processing_rules.json"
        path.write_text(
            json.dumps(config),
            encoding="utf-8",
        )
        return path

    def test_loads_and_validates_default_project_config(self):
        rules = self.job.load_processing_rules()

        self.assertEqual(
            set(rules),
            set(self.job.EXPECTED_STAGE_DELAYS),
        )

        for status, rule in rules.items():
            self.assertEqual(
                rule["next_status"],
                self.job.EXPECTED_STAGE_DELAYS[status],
            )
            self.assertGreater(rule["threshold_seconds"], 0)

    def test_loads_custom_threshold_values(self):
        with tempfile.TemporaryDirectory() as directory:
            config = {
                "stage_delay_thresholds_seconds": {
                    "ORDER_PLACED->ORDER_CONFIRMED": 1,
                    "ORDER_CONFIRMED->ORDER_PACKED": 2,
                    "ORDER_PACKED->ORDER_SHIPPED": 3,
                    "ORDER_SHIPPED->OUT_FOR_DELIVERY": 4,
                    "OUT_FOR_DELIVERY->ORDER_DELIVERED": 5,
                }
            }
            path = self.write_config(directory, config)

            rules = self.job.load_processing_rules(path)

            self.assertEqual(
                rules["ORDER_PLACED"]["threshold_seconds"],
                1,
            )
            self.assertEqual(
                rules["OUT_FOR_DELIVERY"]["threshold_seconds"],
                5,
            )

    def test_rejects_missing_stage(self):
        config = {
            "stage_delay_thresholds_seconds": {
                "ORDER_PLACED->ORDER_CONFIRMED": 1,
            }
        }

        with self.assertRaisesRegex(
            ValueError,
            "Missing stage delay thresholds",
        ):
            self.job.validate_processing_rules(config)

    def test_rejects_unexpected_stage(self):
        config = {
            "stage_delay_thresholds_seconds": {
                "ORDER_PLACED->ORDER_CONFIRMED": 1,
                "ORDER_CONFIRMED->ORDER_PACKED": 2,
                "ORDER_PACKED->ORDER_SHIPPED": 3,
                "ORDER_SHIPPED->OUT_FOR_DELIVERY": 4,
                "OUT_FOR_DELIVERY->ORDER_DELIVERED": 5,
                "ORDER_DELIVERED->ORDER_PLACED": 6,
            }
        }

        with self.assertRaisesRegex(
            ValueError,
            "Unexpected stage delay thresholds",
        ):
            self.job.validate_processing_rules(config)

    def test_rejects_non_integer_and_boolean_thresholds(self):
        for value in ("5", 5.0, True, None):
            with self.subTest(value=value):
                config = json.loads(json.dumps(TEST_RULES))
                config["stage_delay_thresholds_seconds"][
                    "ORDER_PLACED->ORDER_CONFIRMED"
                ] = value

                with self.assertRaisesRegex(
                    ValueError,
                    "integer",
                ):
                    self.job.validate_processing_rules(config)

    def test_rejects_non_positive_thresholds(self):
        for value in (0, -1):
            with self.subTest(value=value):
                config = json.loads(json.dumps(TEST_RULES))
                config["stage_delay_thresholds_seconds"][
                    "ORDER_PLACED->ORDER_CONFIRMED"
                ] = value

                with self.assertRaisesRegex(
                    ValueError,
                    "greater than zero",
                ):
                    self.job.validate_processing_rules(config)

    def test_rejects_invalid_json(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "processing_rules.json"
            path.write_text("not-json", encoding="utf-8")

            with self.assertRaisesRegex(
                ValueError,
                "Unable to load processing rules",
            ):
                self.job.load_processing_rules(path)

    def test_explicit_path_and_environment_override(self):
        with tempfile.TemporaryDirectory() as directory:
            config = json.loads(json.dumps(TEST_RULES))
            path = self.write_config(directory, config)

            with patch.dict(
                os.environ,
                {
                    self.job.STAGE_DELAY_CONFIG_ENV: str(path),
                },
            ):
                rules = self.job.load_processing_rules()

            self.assertEqual(
                rules["ORDER_PLACED"]["threshold_seconds"],
                STAGE_THRESHOLD_SECONDS,
            )


class DelayAlertTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.job = load_job_module()
        cls.rules = cls.job.validate_processing_rules(TEST_RULES)

    def test_alert_record_contains_required_fields(self):
        alert = self.job.build_stage_delay_alert(
            order_id="ORD-ALERT-1",
            current_status="ORDER_PLACED",
            stage_started_at="2026-01-01T12:00:00Z",
            stage_started_processing_at=(
                "2026-01-01T12:05:00Z"
            ),
            elapsed_seconds=6.25,
            threshold_seconds=5,
            alert_type="STAGE_DELAYED",
        )

        self.assertEqual(
            alert["exception_type"],
            "STAGE_DELAYED",
        )
        self.assertEqual(
            alert["exception_category"],
            "DELAY_ALERTS",
        )
        self.assertEqual(alert["alert_type"], "STAGE_DELAYED")
        self.assertEqual(alert["order_id"], "ORD-ALERT-1")
        self.assertEqual(
            alert["current_status"],
            "ORDER_PLACED",
        )
        self.assertEqual(
            alert["stage_started_at"],
            "2026-01-01T12:00:00Z",
        )
        self.assertEqual(alert["elapsed_seconds"], 6.25)
        self.assertEqual(alert["threshold_seconds"], 5)
        self.assertTrue(alert["alerted_at"].endswith("Z"))

    def test_delivery_stage_uses_delivery_alert_type(self):
        self.assertEqual(
            self.job.get_delay_alert_type("OUT_FOR_DELIVERY"),
            "ORDER_DELIVERY_DELAYED",
        )
        self.assertEqual(
            self.job.get_delay_alert_type("ORDER_PLACED"),
            "STAGE_DELAYED",
        )

    def test_accepted_stage_registers_processing_timer(self):
        lifecycle, ctx, output, _ = accept_placed(
            self.job,
            self.rules,
            processing_time=1000000,
        )

        self.assertEqual(
            ctx.timers.processing_timers,
            [1000000 + STAGE_THRESHOLD_SECONDS * 1000],
        )
        self.assertEqual(
            lifecycle.stage_started_processing_ms.value(),
            1000000,
        )
        self.assertEqual(
            lifecycle.stage_alert_deadline_ms.value(),
            1005000,
        )
        self.assertFalse(lifecycle.stage_alert_sent.value())
        self.assertFalse(
            any(
                isinstance(item, tuple)
                and item[0] is self.job.DELAY_ALERTS
                for item in output
            )
        )

    def test_processing_timer_emits_one_alert_with_processing_elapsed(self):
        lifecycle, ctx, _, placed = accept_placed(
            self.job,
            self.rules,
            processing_time=1000000,
        )
        deadline = lifecycle.stage_alert_deadline_ms.value()

        ctx.timers.processing_time = deadline + 1000
        ctx.domain = "PROCESSING_TIME"

        output = list(lifecycle.on_timer(deadline, ctx))

        self.assertEqual(len(output), 1)

        tag, payload = output[0]

        self.assertIs(tag, self.job.DELAY_ALERTS)

        alert = json.loads(payload)

        self.assertEqual(
            alert["alert_type"],
            "STAGE_DELAYED",
        )
        self.assertEqual(
            alert["current_status"],
            "ORDER_PLACED",
        )
        self.assertEqual(alert["order_id"], "ORD-TEST-1")
        self.assertEqual(
            alert["stage_started_at"],
            placed["event_timestamp"],
        )
        self.assertGreaterEqual(
            alert["elapsed_seconds"],
            STAGE_THRESHOLD_SECONDS + 1,
        )
        self.assertEqual(
            alert["threshold_seconds"],
            STAGE_THRESHOLD_SECONDS,
        )
        self.assertIsNone(
            lifecycle.stage_alert_deadline_ms.value()
        )

        second_output = list(
            lifecycle.on_timer(deadline, ctx)
        )

        self.assertEqual(second_output, [])

    def test_advancing_stage_cancels_old_timer_and_uses_new_stage(self):
        lifecycle, ctx, _, placed = accept_placed(
            self.job,
            self.rules,
            processing_time=1000000,
        )
        old_deadline = (
            lifecycle.stage_alert_deadline_ms.value()
        )
        confirmed_at = BASE_TIME + timedelta(seconds=2)
        confirmed = build_event(
            "ORDER_CONFIRMED",
            confirmed_at,
        )
        confirmed_ms = self.job.parse_event_timestamp(
            confirmed["event_timestamp"]
        )

        lifecycle.pending_events = FakeListState(
            [json.dumps(confirmed)]
        )
        ctx.timers.processing_time = 1002000
        ctx.domain = "EVENT_TIME"

        output = list(
            lifecycle.on_timer(confirmed_ms, ctx)
        )

        self.assertIn(
            old_deadline,
            ctx.timers.deleted_processing_timers,
        )
        self.assertEqual(
            ctx.timers.processing_timers[-1],
            1002000 + STAGE_THRESHOLD_SECONDS * 1000,
        )
        self.assertNotEqual(
            lifecycle.stage_alert_deadline_ms.value(),
            old_deadline,
        )
        self.assertEqual(
            lifecycle.current_status.value(),
            "ORDER_CONFIRMED",
        )
        self.assertFalse(
            any(
                isinstance(item, tuple)
                and item[0] is self.job.DELAY_ALERTS
                for item in output
            )
        )

        ctx.domain = "PROCESSING_TIME"
        stale_output = list(
            lifecycle.on_timer(old_deadline, ctx)
        )

        self.assertEqual(stale_output, [])

    def test_pending_next_stage_within_event_time_suppresses_alert(self):
        lifecycle, ctx, _, placed = accept_placed(
            self.job,
            self.rules,
            processing_time=1000000,
        )
        deadline = lifecycle.stage_alert_deadline_ms.value()
        confirmed = build_event(
            "ORDER_CONFIRMED",
            BASE_TIME + timedelta(
                seconds=STAGE_THRESHOLD_SECONDS
            ),
        )

        lifecycle.pending_events = FakeListState(
            [json.dumps(confirmed)]
        )
        ctx.timers.processing_time = deadline + 1000
        ctx.domain = "PROCESSING_TIME"

        output = list(lifecycle.on_timer(deadline, ctx))

        self.assertEqual(output, [])
        self.assertTrue(lifecycle.stage_alert_sent.value())
        self.assertIsNone(
            lifecycle.stage_alert_deadline_ms.value()
        )

    def test_pending_next_stage_beyond_event_time_threshold_alerts(self):
        lifecycle, ctx, _, _ = accept_placed(
            self.job,
            self.rules,
            processing_time=1000000,
        )
        deadline = lifecycle.stage_alert_deadline_ms.value()
        confirmed = build_event(
            "ORDER_CONFIRMED",
            BASE_TIME + timedelta(
                seconds=STAGE_THRESHOLD_SECONDS + 1
            ),
        )

        lifecycle.pending_events = FakeListState(
            [json.dumps(confirmed)]
        )
        ctx.timers.processing_time = deadline + 1000
        ctx.domain = "PROCESSING_TIME"

        output = list(lifecycle.on_timer(deadline, ctx))

        self.assertEqual(len(output), 1)
        self.assertIs(output[0][0], self.job.DELAY_ALERTS)

    def test_pending_cancellation_suppresses_alert(self):
        lifecycle, ctx, _, _ = accept_placed(
            self.job,
            self.rules,
            processing_time=1000000,
        )
        deadline = lifecycle.stage_alert_deadline_ms.value()
        cancelled = build_event(
            "ORDER_CANCELLED",
            BASE_TIME + timedelta(minutes=30),
        )

        lifecycle.pending_events = FakeListState(
            [json.dumps(cancelled)]
        )
        ctx.timers.processing_time = deadline + 1000
        ctx.domain = "PROCESSING_TIME"

        output = list(lifecycle.on_timer(deadline, ctx))

        self.assertEqual(output, [])

    def test_terminal_delivered_state_does_not_alert(self):
        lifecycle = make_lifecycle(
            self.job,
            self.rules,
            status="ORDER_DELIVERED",
            last_timestamp=iso(BASE_TIME),
        )
        deadline = 1005000
        lifecycle.stage_alert_deadline_ms = FakeValueState(
            deadline
        )
        lifecycle.stage_alert_sent = FakeValueState(False)
        ctx = FakeContext(
            processing_time=deadline + 1000,
            time_domain="PROCESSING_TIME",
        )

        output = list(lifecycle.on_timer(deadline, ctx))

        self.assertEqual(output, [])
        self.assertIsNone(
            lifecycle.stage_alert_deadline_ms.value()
        )

    def test_cancelled_order_cancels_timer_without_new_alert(self):
        lifecycle, ctx, _, _ = accept_placed(
            self.job,
            self.rules,
            processing_time=1000000,
        )
        old_deadline = (
            lifecycle.stage_alert_deadline_ms.value()
        )
        cancelled = build_event(
            "ORDER_CANCELLED",
            BASE_TIME + timedelta(minutes=1),
        )
        cancelled_ms = self.job.parse_event_timestamp(
            cancelled["event_timestamp"]
        )

        lifecycle.pending_events = FakeListState(
            [json.dumps(cancelled)]
        )
        ctx.domain = "EVENT_TIME"

        output = list(
            lifecycle.on_timer(cancelled_ms, ctx)
        )

        self.assertIn(
            old_deadline,
            ctx.timers.deleted_processing_timers,
        )
        self.assertEqual(
            lifecycle.current_status.value(),
            "ORDER_CANCELLED",
        )
        self.assertEqual(
            ctx.timers.processing_timers,
            [old_deadline],
        )
        self.assertFalse(
            any(
                isinstance(item, tuple)
                and item[0] is self.job.DELAY_ALERTS
                for item in output
            )
        )


class EventTimeCompatibilityTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.job = load_job_module()
        cls.rules = cls.job.validate_processing_rules(TEST_RULES)

    def test_out_of_order_events_use_event_time_durations(self):
        events = [
            build_event(
                "ORDER_PLACED",
                BASE_TIME,
            ),
            build_event(
                "ORDER_PACKED",
                BASE_TIME + timedelta(seconds=12),
            ),
            build_event(
                "ORDER_CONFIRMED",
                BASE_TIME + timedelta(seconds=10),
            ),
            build_event(
                "ORDER_SHIPPED",
                BASE_TIME + timedelta(seconds=30),
            ),
            build_event(
                "OUT_FOR_DELIVERY",
                BASE_TIME + timedelta(seconds=40),
            ),
            build_event(
                "ORDER_DELIVERED",
                BASE_TIME + timedelta(seconds=50),
            ),
        ]
        max_timestamp = max(
            self.job.parse_event_timestamp(
                event["event_timestamp"]
            )
            for event in events
        )

        lifecycle = make_lifecycle(
            self.job,
            self.rules,
            pending=[
                json.dumps(event) for event in events
            ],
        )
        ctx = FakeContext(
            watermark=max_timestamp - 1,
            processing_time=1000000,
        )

        output = list(
            lifecycle.on_timer(max_timestamp, ctx)
        )
        processed = [
            json.loads(item)
            for item in output
            if not isinstance(item, tuple)
        ]

        self.assertEqual(
            [record["event_type"] for record in processed],
            [
                "ORDER_PLACED",
                "ORDER_CONFIRMED",
                "ORDER_PACKED",
                "ORDER_SHIPPED",
                "OUT_FOR_DELIVERY",
                "ORDER_DELIVERED",
            ],
        )

        durations = {
            record["event_type"]: (
                record["stage_duration_seconds"]
            )
            for record in processed
        }

        self.assertIsNone(durations["ORDER_PLACED"])
        self.assertEqual(durations["ORDER_CONFIRMED"], 10)
        self.assertEqual(durations["ORDER_PACKED"], 2)
        self.assertEqual(durations["ORDER_SHIPPED"], 18)
        self.assertEqual(
            durations["OUT_FOR_DELIVERY"],
            10,
        )
        self.assertEqual(
            durations["ORDER_DELIVERED"],
            10,
        )

        delivered = processed[-1]

        self.assertEqual(
            delivered["total_delivery_duration_seconds"],
            50,
        )
        self.assertEqual(
            delivered["total_delivery_duration_minutes"],
            round(50 / 60.0, 3),
        )
        self.assertFalse(
            any(
                isinstance(item, tuple)
                and item[0] is self.job.DELAY_ALERTS
                for item in output
            )
        )

    def test_late_event_is_still_isolated(self):
        lifecycle = make_lifecycle(
            self.job,
            self.rules,
        )
        late = build_event("ORDER_PLACED", BASE_TIME)
        late_ms = self.job.parse_event_timestamp(
            late["event_timestamp"]
        )
        ctx = FakeContext(watermark=late_ms + 1)

        output = list(
            lifecycle.process_element(
                json.dumps(late),
                ctx,
            )
        )

        self.assertEqual(len(output), 1)
        self.assertIs(
            output[0][0],
            self.job.LIFECYCLE_EXCEPTIONS,
        )
        self.assertEqual(
            json.loads(output[0][1])["exception_type"],
            "EVENT_TOO_LATE",
        )

    def test_heartbeat_is_not_a_business_event(self):
        heartbeat = build_event(
            "WATERMARK_HEARTBEAT",
            BASE_TIME,
            source="system",
        )
        business = build_event("ORDER_PLACED", BASE_TIME)

        self.assertFalse(
            self.job.is_business_event(
                json.dumps(heartbeat)
            )
        )
        self.assertTrue(
            self.job.is_business_event(
                json.dumps(business)
            )
        )


if __name__ == "__main__":
    unittest.main()
