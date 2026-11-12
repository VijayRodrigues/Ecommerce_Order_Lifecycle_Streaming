"""Dashboard configuration with local-first defaults."""

import os

from dataclasses import dataclass, field
from pathlib import Path


def _bool_env(name, default):
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _int_env(name, default):
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


@dataclass(frozen=True)
class Settings:
    repo_root: Path
    docker_lab_root: Path
    bootstrap_servers: str
    input_topic: str
    state_topic: str
    processed_topic: str
    exceptions_topic: str
    state_group_id: str
    processed_group_id: str
    exceptions_group_id: str
    flink_rest_url: str
    flink_job_name: str
    flink_jobmanager_container: str
    flink_taskmanager_container: str
    kafka_container: str
    flink_compose_file: Path
    kafka_compose_file: Path
    dashboard_host: str
    dashboard_port: int
    kafka_start_from_beginning: bool
    processed_buffer_size: int
    exceptions_buffer_size: int
    state_buffer_size: int
    order_event_buffer_size: int
    activity_buffer_size: int
    generator_max_finite_seconds: int
    command_timeout_seconds: int
    docker_cache_seconds: float
    flink_cache_seconds: float
    kafka_cache_seconds: float
    project_version: str = field(default="0.1.0")

    @classmethod
    def from_env(cls):
        repo_root = Path(__file__).resolve().parents[3]
        docker_lab_root = Path(
            os.getenv("DOCKERLAB_ROOT", r"F:\DockerLab")
        )

        return cls(
            repo_root=repo_root,
            docker_lab_root=docker_lab_root,
            bootstrap_servers=os.getenv(
                "KAFKA_BOOTSTRAP_SERVERS",
                "localhost:9092",
            ),
            input_topic=os.getenv(
                "KAFKA_INPUT_TOPIC",
                "ecommerce.order-lifecycle.events",
            ),
            state_topic=os.getenv(
                "KAFKA_STATE_TOPIC",
                "ecommerce.order-lifecycle.state",
            ),
            processed_topic=os.getenv(
                "KAFKA_PROCESSED_TOPIC",
                "ecommerce.order-lifecycle.processed",
            ),
            exceptions_topic=os.getenv(
                "KAFKA_EXCEPTIONS_TOPIC",
                "ecommerce.order-lifecycle.exceptions",
            ),
            state_group_id=os.getenv(
                "DASHBOARD_STATE_CONSUMER_GROUP",
                "dashboard-order-state-monitor",
            ),
            processed_group_id=os.getenv(
                "DASHBOARD_PROCESSED_CONSUMER_GROUP",
                "dashboard-processed-events-monitor",
            ),
            exceptions_group_id=os.getenv(
                "DASHBOARD_EXCEPTIONS_CONSUMER_GROUP",
                "dashboard-exceptions-monitor",
            ),
            flink_rest_url=os.getenv(
                "FLINK_JOBMANAGER_REST_URL",
                "http://localhost:18081",
            ).rstrip("/"),
            flink_job_name=os.getenv(
                "FLINK_JOB_NAME",
                "Ecommerce Order Lifecycle Processor",
            ),
            flink_jobmanager_container=os.getenv(
                "FLINK_JOBMANAGER_CONTAINER",
                "flink-jobmanager",
            ),
            flink_taskmanager_container=os.getenv(
                "FLINK_TASKMANAGER_CONTAINER",
                "flink-taskmanager",
            ),
            kafka_container=os.getenv(
                "KAFKA_CONTAINER",
                "kafka_kraft_docker",
            ),
            flink_compose_file=docker_lab_root
            / "processing"
            / "flink_docker"
            / "docker-compose.yml",
            kafka_compose_file=docker_lab_root
            / "messaging"
            / "kafka_kraft_docker"
            / "compose.yaml",
            dashboard_host=os.getenv("DASHBOARD_HOST", "127.0.0.1"),
            dashboard_port=_int_env("DASHBOARD_PORT", 8090),
            kafka_start_from_beginning=_bool_env(
                "DASHBOARD_KAFKA_FROM_BEGINNING",
                True,
            ),
            processed_buffer_size=_int_env(
                "DASHBOARD_PROCESSED_BUFFER",
                600,
            ),
            exceptions_buffer_size=_int_env(
                "DASHBOARD_EXCEPTIONS_BUFFER",
                500,
            ),
            state_buffer_size=_int_env(
                "DASHBOARD_STATE_BUFFER",
                5000,
            ),
            order_event_buffer_size=_int_env(
                "DASHBOARD_ORDER_EVENT_BUFFER",
                100,
            ),
            activity_buffer_size=_int_env(
                "DASHBOARD_ACTIVITY_BUFFER",
                250,
            ),
            generator_max_finite_seconds=_int_env(
                "DASHBOARD_GENERATOR_TIMEOUT",
                180,
            ),
            command_timeout_seconds=_int_env(
                "DASHBOARD_COMMAND_TIMEOUT",
                120,
            ),
            docker_cache_seconds=float(
                os.getenv("DASHBOARD_DOCKER_CACHE_SECONDS", "5")
            ),
            flink_cache_seconds=float(
                os.getenv("DASHBOARD_FLINK_CACHE_SECONDS", "3")
            ),
            kafka_cache_seconds=float(
                os.getenv("DASHBOARD_KAFKA_CACHE_SECONDS", "8")
            ),
        )

    @property
    def frontend_dir(self):
        return self.repo_root / "dashboard" / "frontend"

    @property
    def venv_python(self):
        return self.repo_root / ".venv" / "Scripts" / "python.exe"

    def public_config(self):
        return {
            "bootstrap_servers": self.bootstrap_servers,
            "input_topic": self.input_topic,
            "state_topic": self.state_topic,
            "processed_topic": self.processed_topic,
            "exceptions_topic": self.exceptions_topic,
            "consumer_groups": {
                "state": self.state_group_id,
                "processed": self.processed_group_id,
                "exceptions": self.exceptions_group_id,
            },
            "flink_rest_url": self.flink_rest_url,
            "flink_job_name": self.flink_job_name,
            "docker_lab_root": str(self.docker_lab_root),
            "dashboard_host": self.dashboard_host,
            "dashboard_port": self.dashboard_port,
            "from_beginning": self.kafka_start_from_beginning,
        }
