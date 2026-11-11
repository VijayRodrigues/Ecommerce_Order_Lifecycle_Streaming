import importlib.util
import sys
import types

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
JOB_PATH = (
    PROJECT_ROOT
    / "src"
    / "order_lifecycle"
    / "flink"
    / "order_lifecycle_job.py"
)


_STUBS_INSTALLED = False


def install_pyflink_stubs():
    global _STUBS_INSTALLED

    if _STUBS_INSTALLED:
        return

    if "pyflink" in sys.modules:
        _STUBS_INSTALLED = True
        return

    if importlib.util.find_spec("pyflink") is not None:
        _STUBS_INSTALLED = True
        return

    def new_module(name):
        module = types.ModuleType(name)
        sys.modules[name] = module
        return module

    class Dummy:
        def __init__(self, *args, **kwargs):
            pass

    class Row:
        def __init__(self, *values):
            self.values = list(values)

        def getField(self, index):
            return self.values[index]

        def __getitem__(self, index):
            return self.values[index]

        def __len__(self):
            return len(self.values)

        def __eq__(self, other):
            return (
                isinstance(other, Row)
                and self.values == other.values
            )

        def __repr__(self):
            return f"Row{tuple(self.values)}"

    class Types:
        @staticmethod
        def STRING():
            return "string"

        @staticmethod
        def BOOLEAN():
            return "boolean"

        @staticmethod
        def LONG():
            return "long"

        @staticmethod
        def ROW_NAMED(names, types):
            return ("row", tuple(names), tuple(types))

    class DeliveryGuarantee:
        AT_LEAST_ONCE = "AT_LEAST_ONCE"
        EXACTLY_ONCE = "EXACTLY_ONCE"
        NONE = "NONE"

    class OutputTag(Dummy):
        pass

    class TimeDomain:
        EVENT_TIME = "EVENT_TIME"
        PROCESSING_TIME = "PROCESSING_TIME"

    pyflink = new_module("pyflink")
    common = new_module("pyflink.common")
    common_time = new_module("pyflink.common.time")
    common_serialization = new_module(
        "pyflink.common.serialization"
    )
    common_watermark = new_module(
        "pyflink.common.watermark_strategy"
    )
    datastream = new_module("pyflink.datastream")
    connectors = new_module("pyflink.datastream.connectors")
    connectors_base = new_module(
        "pyflink.datastream.connectors.base"
    )
    connectors_kafka = new_module(
        "pyflink.datastream.connectors.kafka"
    )
    functions = new_module("pyflink.datastream.functions")
    state = new_module("pyflink.datastream.state")
    java_gateway = new_module("pyflink.java_gateway")

    common.Types = Types
    common.Row = Row
    common.WatermarkStrategy = Dummy
    common.time = common_time
    common.serialization = common_serialization
    common.watermark_strategy = common_watermark
    common_time.Duration = Dummy
    common_serialization.SimpleStringSchema = Dummy
    common_serialization.SerializationSchema = Dummy
    common_watermark.TimestampAssigner = Dummy
    datastream.StreamExecutionEnvironment = Dummy
    datastream.OutputTag = OutputTag
    datastream.TimeDomain = TimeDomain
    datastream.connectors = connectors
    datastream.functions = functions
    datastream.state = state
    connectors.base = connectors_base
    connectors.kafka = connectors_kafka
    connectors_base.DeliveryGuarantee = DeliveryGuarantee
    connectors_kafka.KafkaSource = Dummy
    connectors_kafka.KafkaOffsetsInitializer = Dummy
    connectors_kafka.KafkaSink = Dummy
    connectors_kafka.KafkaRecordSerializationSchema = Dummy
    functions.ProcessFunction = Dummy
    functions.KeyedProcessFunction = Dummy
    state.ValueStateDescriptor = Dummy
    state.ListStateDescriptor = Dummy
    java_gateway.get_gateway = lambda: Dummy()

    pyflink.common = common
    pyflink.datastream = datastream
    pyflink.java_gateway = java_gateway
    _STUBS_INSTALLED = True


def load_job_module():
    install_pyflink_stubs()
    spec = importlib.util.spec_from_file_location(
        "order_lifecycle_job_under_test",
        JOB_PATH,
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
