import unittest

from pyflink_stubs import load_job_module


STAGE_RULES = {
    "stage_delay_thresholds_seconds": {
        "ORDER_PLACED->ORDER_CONFIRMED": 900,
        "ORDER_CONFIRMED->ORDER_PACKED": 3600,
        "ORDER_PACKED->ORDER_SHIPPED": 7200,
        "ORDER_SHIPPED->OUT_FOR_DELIVERY": 14400,
        "OUT_FOR_DELIVERY->ORDER_DELIVERED": 10800,
    }
}


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

    def __init__(self):
        self._values = []

    def get(self):
        return iter(self._values)

    def add(self, value):
        self._values.append(value)

    def update(self, values):
        self._values = list(values)

    def clear(self):
        self._values = []


class FakeRuntimeContext:

    def __init__(self):
        self.value_descriptors = []
        self.list_descriptors = []

    def get_state(self, descriptor):
        self.value_descriptors.append(descriptor)
        return FakeValueState()

    def get_list_state(self, descriptor):
        self.list_descriptors.append(descriptor)
        return FakeListState()


class FakeCheckpointConfig:

    def __init__(self):
        self.checkpoint_timeout = None
        self.min_pause_between_checkpoints = None
        self.max_concurrent_checkpoints = None
        self.tolerable_checkpoint_failure_number = None
        self.checkpointing_mode = None

    def set_checkpoint_timeout(self, timeout):
        self.checkpoint_timeout = timeout
        return self

    def set_min_pause_between_checkpoints(self, pause):
        self.min_pause_between_checkpoints = pause
        return self

    def set_max_concurrent_checkpoints(self, maximum):
        self.max_concurrent_checkpoints = maximum
        return self

    def set_tolerable_checkpoint_failure_number(self, number):
        self.tolerable_checkpoint_failure_number = number
        return self

    def set_checkpointing_mode(self, mode):
        self.checkpointing_mode = mode
        return self


class FakeEnvironment:

    def __init__(self):
        self.configuration = None
        self.checkpoint_interval = None
        self.checkpoint_mode = None
        self.checkpoint_config = FakeCheckpointConfig()

    def configure(self, configuration):
        self.configuration = configuration

    def enable_checkpointing(self, interval, mode=None):
        self.checkpoint_interval = interval
        self.checkpoint_mode = mode
        return self

    def get_checkpoint_config(self):
        return self.checkpoint_config


class CheckpointConfigurationTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.job = load_job_module()

    def test_checkpoint_intervals_and_limits_match_required_defaults(self):
        env = FakeEnvironment()

        checkpoint_config = self.job.configure_recovery(env)

        self.assertEqual(
            env.checkpoint_interval,
            self.job.CHECKPOINT_INTERVAL_MS,
        )
        self.assertEqual(env.checkpoint_interval, 60_000)
        self.assertEqual(
            env.checkpoint_mode,
            self.job.CheckpointingMode.EXACTLY_ONCE,
        )
        self.assertEqual(
            checkpoint_config.checkpoint_timeout,
            300_000,
        )
        self.assertEqual(
            checkpoint_config.min_pause_between_checkpoints,
            10_000,
        )
        self.assertEqual(
            checkpoint_config.max_concurrent_checkpoints,
            1,
        )
        self.assertEqual(
            checkpoint_config.tolerable_checkpoint_failure_number,
            3,
        )
        self.assertEqual(
            checkpoint_config.checkpointing_mode,
            self.job.CheckpointingMode.EXACTLY_ONCE,
        )

    def test_checkpoint_storage_is_shared_durable_container_path(self):
        configuration = (
            self.job.build_recovery_configuration()
        )
        values = configuration.to_dict()

        self.assertEqual(
            values["execution.checkpointing.storage"],
            "filesystem",
        )
        self.assertEqual(
            values["execution.checkpointing.dir"],
            "file:///opt/flink/usrlib/flink-state/checkpoints",
        )
        self.assertEqual(
            values["execution.checkpointing.savepoint-dir"],
            "file:///opt/flink/usrlib/flink-state/savepoints",
        )
        self.assertNotIn("/tmp/", values["execution.checkpointing.dir"])
        self.assertNotIn(
            "/tmp/",
            values["execution.checkpointing.savepoint-dir"],
        )

    def test_state_backend_is_hashmap(self):
        values = (
            self.job.build_recovery_configuration().to_dict()
        )

        self.assertEqual(
            values["state.backend.type"],
            "hashmap",
        )

    def test_restart_strategy_is_bounded_fixed_delay(self):
        values = (
            self.job.build_recovery_configuration().to_dict()
        )

        self.assertEqual(
            values["restart-strategy.type"],
            "fixed-delay",
        )
        self.assertEqual(
            values["restart-strategy.fixed-delay.attempts"],
            "10",
        )
        self.assertEqual(
            values["restart-strategy.fixed-delay.delay"],
            "10 s",
        )


class StateDescriptorCompatibilityTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.job = load_job_module()

    def test_deduplication_state_descriptor_is_stable(self):
        deduplicate = self.job.DeduplicateEvents()
        runtime = FakeRuntimeContext()

        deduplicate.open(runtime)

        self.assertEqual(len(runtime.value_descriptors), 1)
        descriptor = runtime.value_descriptors[0]

        self.assertEqual(descriptor.name, "event_seen")
        self.assertEqual(descriptor.type_info, "boolean")

    def test_lifecycle_state_descriptor_names_and_types_are_stable(self):
        lifecycle = self.job.ValidateLifecycle(
            self.job.validate_processing_rules(STAGE_RULES)
        )
        runtime = FakeRuntimeContext()

        lifecycle.open(runtime)

        descriptors = {
            descriptor.name: descriptor
            for descriptor in runtime.value_descriptors
        }

        self.assertEqual(
            set(descriptors),
            self.job.VALIDATE_LIFECYCLE_STATE_NAMES
            - {"pending_order_events"},
        )
        self.assertEqual(
            descriptors["current_order_status"].type_info,
            "string",
        )
        self.assertEqual(
            descriptors["last_accepted_event_timestamp"].type_info,
            "string",
        )
        self.assertEqual(
            descriptors["order_placed_timestamp"].type_info,
            "string",
        )
        self.assertEqual(
            descriptors["stage_started_processing_ms"].type_info,
            "long",
        )
        self.assertEqual(
            descriptors["stage_alert_deadline_ms"].type_info,
            "long",
        )
        self.assertEqual(
            descriptors["stage_alert_sent"].type_info,
            "boolean",
        )

        self.assertEqual(len(runtime.list_descriptors), 1)
        pending = runtime.list_descriptors[0]

        self.assertEqual(pending.name, "pending_order_events")
        self.assertEqual(pending.type_info, "string")

    def test_all_lifecycle_state_uses_flink_managed_state(self):
        expected = {
            "current_order_status",
            "pending_order_events",
            "last_accepted_event_timestamp",
            "order_placed_timestamp",
            "stage_started_processing_ms",
            "stage_alert_deadline_ms",
            "stage_alert_sent",
        }

        self.assertEqual(
            self.job.VALIDATE_LIFECYCLE_STATE_NAMES,
            expected,
        )


if __name__ == "__main__":
    unittest.main()
