import importlib.util
import json
import sys
import types
import unittest
import uuid

from datetime import datetime, timedelta, timezone
from pathlib import Path

from order_lifecycle.generator.event_generator import (
    WATERMARK_HEARTBEAT,
    WATERMARK_HEARTBEAT_DELAY_SECONDS,
    add_watermark_heartbeat,
    generate_scenario,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_PATH = PROJECT_ROOT / "schemas" / "order_event.schema.json"
JOB_PATH = (
    PROJECT_ROOT
    / "src"
    / "order_lifecycle"
    / "flink"
    / "order_lifecycle_job.py"
)

ALLOWED_LATENESS_SECONDS = 300


def parse_timestamp(event):
    return datetime.fromisoformat(
        event["event_timestamp"].replace("Z", "+00:00")
    )


def install_pyflink_stubs():
    if importlib.util.find_spec("pyflink") is not None:
        return

    def new_module(name):
        module = types.ModuleType(name)
        sys.modules[name] = module
        return module

    class Dummy:
        def __init__(self, *args, **kwargs):
            pass

    class Types:
        @staticmethod
        def STRING():
            return "string"

        @staticmethod
        def BOOLEAN():
            return "boolean"

    class OutputTag(Dummy):
        pass

    pyflink = new_module("pyflink")
    common = new_module("pyflink.common")
    common_time = new_module("pyflink.common.time")
    common_serialization = new_module("pyflink.common.serialization")
    common_watermark = new_module("pyflink.common.watermark_strategy")
    datastream = new_module("pyflink.datastream")
    connectors = new_module("pyflink.datastream.connectors")
    connectors_kafka = new_module("pyflink.datastream.connectors.kafka")
    functions = new_module("pyflink.datastream.functions")
    state = new_module("pyflink.datastream.state")

    common.Types = Types
    common.WatermarkStrategy = Dummy
    common.time = common_time
    common.serialization = common_serialization
    common.watermark_strategy = common_watermark
    common_time.Duration = Dummy
    common_serialization.SimpleStringSchema = Dummy
    common_watermark.TimestampAssigner = Dummy
    datastream.StreamExecutionEnvironment = Dummy
    datastream.OutputTag = OutputTag
    datastream.connectors = connectors
    datastream.functions = functions
    datastream.state = state
    connectors.kafka = connectors_kafka
    connectors_kafka.KafkaSource = Dummy
    connectors_kafka.KafkaOffsetsInitializer = Dummy
    functions.ProcessFunction = Dummy
    functions.KeyedProcessFunction = Dummy
    state.ValueStateDescriptor = Dummy
    state.ListStateDescriptor = Dummy

    pyflink.common = common
    pyflink.datastream = datastream


def load_job_module():
    install_pyflink_stubs()
    spec = importlib.util.spec_from_file_location(
        "order_lifecycle_job_under_test",
        JOB_PATH,
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class GeneratorHeartbeatTests(unittest.TestCase):

    def setUp(self):
        self.base = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

    def test_heartbeat_metadata(self):
        scenarios = [
            "normal_flow",
            "duplicate_events",
            "late_events",
            "out_of_order_events",
            "invalid_events",
        ]

        for scenario in scenarios:
            with self.subTest(scenario=scenario):
                events = generate_scenario(scenario, start_time=self.base)
                heartbeat = add_watermark_heartbeat(events)[-1]

                self.assertEqual(
                    heartbeat["event_type"],
                    WATERMARK_HEARTBEAT,
                )
                self.assertEqual(heartbeat["source"], "system")
                self.assertEqual(heartbeat["schema_version"], "1.0")
                self.assertEqual(
                    heartbeat["order_id"],
                    events[0]["order_id"],
                )
                uuid.UUID(heartbeat["event_id"])

    def test_heartbeat_is_ahead_of_max_event_timestamp(self):
        events = generate_scenario(
            "out_of_order_events",
            start_time=self.base,
        )
        heartbeat = add_watermark_heartbeat(events)[-1]
        max_timestamp = max(parse_timestamp(event) for event in events)

        self.assertEqual(
            (parse_timestamp(heartbeat) - max_timestamp).total_seconds(),
            WATERMARK_HEARTBEAT_DELAY_SECONDS,
        )

    def test_schema_recognizes_control_event_and_source(self):
        schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
        events = generate_scenario("normal_flow", start_time=self.base)
        heartbeat = add_watermark_heartbeat(events)[-1]

        self.assertIn(
            heartbeat["event_type"],
            schema["properties"]["event_type"]["enum"],
        )
        self.assertIn(
            heartbeat["source"],
            schema["properties"]["source"]["enum"],
        )

    def test_single_heartbeat_with_unique_id(self):
        events = generate_scenario("normal_flow", start_time=self.base)
        first = add_watermark_heartbeat(events)
        second = add_watermark_heartbeat(events)

        self.assertEqual(
            [event["event_type"] for event in first].count(
                WATERMARK_HEARTBEAT
            ),
            1,
        )
        self.assertNotEqual(
            first[-1]["event_id"],
            second[-1]["event_id"],
        )

    def test_generate_scenario_excludes_heartbeat(self):
        events = generate_scenario("normal_flow", start_time=self.base)

        self.assertNotIn(
            WATERMARK_HEARTBEAT,
            [event["event_type"] for event in events],
        )

    def test_out_of_order_keeps_two_minute_inversion(self):
        events = generate_scenario(
            "out_of_order_events",
            start_time=self.base,
        )
        packed = events[1]
        confirmed = events[2]

        self.assertEqual(packed["event_type"], "ORDER_PACKED")
        self.assertEqual(confirmed["event_type"], "ORDER_CONFIRMED")
        self.assertEqual(
            (parse_timestamp(packed) - parse_timestamp(confirmed))
            .total_seconds(),
            120,
        )

    def test_late_scenario_final_business_event_is_late(self):
        events = generate_scenario("late_events", start_time=self.base)
        max_timestamp = max(
            parse_timestamp(event) for event in events[:-1]
        )

        self.assertEqual(events[-1]["event_type"], "ORDER_PACKED")
        self.assertLess(parse_timestamp(events[-1]), max_timestamp)


class JobHeartbeatTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.job = load_job_module()
        cls.base = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

    def heartbeat(self):
        events = generate_scenario("normal_flow", start_time=self.base)
        return add_watermark_heartbeat(events)[-1]

    def test_heartbeat_passes_validation(self):
        heartbeat = self.heartbeat()
        output = list(
            self.job.ValidateEvents().process_element(
                json.dumps(heartbeat),
                None,
            )
        )

        self.assertEqual(len(output), 1)
        self.assertEqual(
            json.loads(output[0])["event_type"],
            WATERMARK_HEARTBEAT,
        )

    def test_heartbeat_reaches_timestamp_assignment(self):
        heartbeat = self.heartbeat()
        assigner = self.job.EventTimestampAssigner()
        assigned = assigner.extract_timestamp(
            json.dumps(heartbeat),
            None,
        )

        self.assertEqual(
            assigned,
            int(parse_timestamp(heartbeat).timestamp() * 1000),
        )

    def test_heartbeat_advances_watermark_past_final_event(self):
        events = generate_scenario(
            "out_of_order_events",
            start_time=self.base,
        )
        heartbeat = add_watermark_heartbeat(events)[-1]
        max_business_timestamp = max(
            parse_timestamp(event) for event in events
        )
        watermark = parse_timestamp(heartbeat) - timedelta(
            seconds=ALLOWED_LATENESS_SECONDS
        )

        self.assertGreaterEqual(watermark, max_business_timestamp)

    def test_heartbeat_is_excluded_from_lifecycle_events(self):
        heartbeat = self.heartbeat()
        business = generate_scenario(
            "normal_flow",
            start_time=self.base,
        )[0]

        self.assertFalse(
            self.job.is_business_event(json.dumps(heartbeat))
        )
        self.assertTrue(
            self.job.is_business_event(json.dumps(business))
        )

    def test_job_recognizes_heartbeat_and_system_source(self):
        self.assertIn(WATERMARK_HEARTBEAT, self.job.EVENT_TYPES)
        self.assertIn("system", self.job.SOURCES)


if __name__ == "__main__":
    unittest.main()
