import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from dashboard.backend.app.container import create_services
from dashboard.backend.app.main import create_app
from dashboard.backend.app.services import order_service
from dashboard.backend.app.services.docker_service import DockerService
from dashboard.backend.app.services.flink_service import FlinkService
from dashboard.backend.app.services.generator_service import build_generator_command
from dashboard.backend.app.services.kafka_monitor import KafkaMonitor
from dashboard.backend.app.services.processes import CommandResult, validate_docker_command


class _Hub:
    def publish_threadsafe(self, _message):
        return None


class _Activity:
    def __init__(self):
        self.entries = []

    def add(self, level, source, message, detail=None):
        self.entries.append(
            {
                "level": level,
                "source": source,
                "message": message,
                "detail": detail,
            }
        )
        return self.entries[-1]

    def recent(self, limit=100):
        return self.entries[:limit]


def _settings():
    return SimpleNamespace(
        bootstrap_servers="localhost:9092",
        input_topic="ecommerce.order-lifecycle.events",
        state_topic="ecommerce.order-lifecycle.state",
        processed_topic="ecommerce.order-lifecycle.processed",
        exceptions_topic="ecommerce.order-lifecycle.exceptions",
        state_group_id="dashboard-order-state-monitor",
        processed_group_id="dashboard-processed-events-monitor",
        exceptions_group_id="dashboard-exceptions-monitor",
        kafka_start_from_beginning=True,
        processed_buffer_size=100,
        exceptions_buffer_size=100,
        state_buffer_size=200,
        order_event_buffer_size=50,
        activity_buffer_size=100,
        flink_rest_url="http://localhost:18081",
        flink_job_name="Ecommerce Order Lifecycle Processor",
        flink_jobmanager_container="flink-jobmanager",
        flink_taskmanager_container="flink-taskmanager",
        kafka_container="kafka_kraft_docker",
        docker_cache_seconds=0,
        flink_cache_seconds=0,
        kafka_cache_seconds=0,
        command_timeout_seconds=5,
        generator_max_finite_seconds=30,
        repo_root=Path("."),
        venv_python=Path(".venv/Scripts/python.exe"),
        frontend_dir=Path("dashboard/frontend"),
        project_version="test",
        dashboard_host="127.0.0.1",
        dashboard_port=8090,
        docker_lab_root=Path("F:/DockerLab"),
        flink_compose_file=Path("F:/DockerLab/processing/flink_docker/docker-compose.yml"),
        kafka_compose_file=Path("F:/DockerLab/messaging/kafka_kraft_docker/compose.yaml"),
        public_config=lambda: {},
    )


class DashboardBackendTests(unittest.TestCase):
    def test_health_and_config_endpoints(self):
        services = create_services(start_background=False)
        app = create_app(services)
        from fastapi.testclient import TestClient

        with TestClient(app) as client:
            health = client.get("/api/health")
            config = client.get("/api/config")
        self.assertEqual(health.status_code, 200)
        self.assertEqual(health.json()["status"], "ok")
        self.assertEqual(config.status_code, 200)
        self.assertIn("state_topic", config.json())

    def test_websocket_live_connection(self):
        services = create_services(start_background=False)
        app = create_app(services)
        from fastapi.testclient import TestClient

        with TestClient(app) as client:
            with client.websocket_connect("/ws/live") as ws:
                msg = ws.receive_json()
                self.assertEqual(msg["type"], "connected")

    def test_docker_command_allowlist(self):
        self.assertEqual(
            validate_docker_command(["docker", "inspect", "x"])[0],
            "docker",
        )
        self.assertEqual(
            validate_docker_command(["docker", "compose", "-f", "x.yml", "up", "-d"])[1],
            "compose",
        )
        for blocked in (
            ["docker", "compose", "down"],
            ["docker", "rm", "x"],
            ["docker", "volume", "rm", "x"],
            ["sh", "-c", "echo hi"],
        ):
            with self.assertRaises(ValueError):
                validate_docker_command(blocked)

    def test_generator_command_uses_existing_producer(self):
        cmd = build_generator_command(Path(".venv/Scripts/python.exe"), "normal_flow", 2)
        self.assertIn("order_lifecycle.kafka.producer", cmd)
        self.assertIn("--scenario", cmd)
        self.assertIn("normal_flow", cmd)
        with self.assertRaises(ValueError):
            build_generator_command(Path("python"), "bad_scenario", 1)

    def test_flink_status_handles_unavailable_and_job(self):
        activity = _Activity()
        service = FlinkService(_settings(), activity)
        with patch.object(
            service,
            "_request",
            side_effect=OSError("no flink"),
        ):
            unavailable = service.status(force=True)
        self.assertFalse(unavailable["available"])
        self.assertIn("no flink", unavailable["error"])

        service2 = FlinkService(_settings(), activity)
        overview = {"flink-version": "1.20.3", "taskmanagers": 1, "slots-total": 4, "slots-available": 3, "jobs-running": 1, "jobs-finished": 0, "jobs-failed": 0}
        jobs = {"jobs": [{"jid": "a" * 32, "name": "Ecommerce Order Lifecycle Processor", "state": "RUNNING", "duration": 5000, "tasks": {"running": 4, "total": 4}}]}
        checkpoints = {"counts": {"completed": 2}, "latest": {"completed": {"id": 7, "status": "COMPLETED", "trigger_timestamp": 1, "state_size": 10, "end_to_end_duration": 5}}, "history": []}
        def req(path, method="GET", timeout=5):
            if path == "/overview":
                return overview
            if path == "/jobs/overview":
                return jobs
            if path.endswith("/checkpoints"):
                return checkpoints
            raise AssertionError(path)
        with patch.object(service2, "_request", side_effect=req):
            status = service2.status(force=True)
        self.assertTrue(status["available"])
        self.assertTrue(status["job"]["running"])
        self.assertEqual(status["checkpoints"]["latest"]["completed"]["id"], 7)

    def test_kafka_message_parsing_and_state_aggregation(self):
        monitor = KafkaMonitor(_settings(), _Hub(), _Activity())

        processed = SimpleNamespace(
            value=json.dumps(
                {
                    "event_id": "evt-1",
                    "order_id": "ORD-1",
                    "event_type": "ORDER_CONFIRMED",
                    "previous_status": "ORDER_PLACED",
                    "current_status": "ORDER_CONFIRMED",
                    "event_timestamp": "2026-11-13T10:00:00Z",
                    "processed_at": "2026-11-13T10:00:01Z",
                    "stage_duration_seconds": 60,
                }
            ).encode("utf-8"),
            key=b"ORD-1",
            topic="ecommerce.order-lifecycle.processed",
            partition=0,
            offset=1,
        )
        monitor._handle_record({"kind": "processed", "topic": processed.topic, "group_id": "g"}, processed)

        state = SimpleNamespace(
            value=json.dumps(
                {
                    "order_id": "ORD-1",
                    "current_status": "ORDER_CONFIRMED",
                    "previous_status": "ORDER_PLACED",
                    "last_event_timestamp": "2026-11-13T10:00:00Z",
                    "updated_at": "2026-11-13T10:00:01Z",
                }
            ).encode("utf-8"),
            key=b"ORD-1",
            topic="ecommerce.order-lifecycle.state",
            partition=0,
            offset=1,
        )
        monitor._handle_record({"kind": "state", "topic": state.topic, "group_id": "g"}, state)

        malformed = SimpleNamespace(
            value=b"{not-json",
            key=b"x",
            topic="ecommerce.order-lifecycle.exceptions",
            partition=0,
            offset=2,
        )
        monitor._handle_record({"kind": "exceptions", "topic": malformed.topic, "group_id": "g"}, malformed)

        detail = monitor.order_detail("ORD-1")
        self.assertTrue(detail["found"])
        self.assertEqual(detail["summary"]["current_status"], "ORDER_CONFIRMED")
        metrics = monitor.metrics()
        self.assertEqual(metrics["processed_events"], 1)
        self.assertEqual(metrics["state_updates"], 1)
        self.assertEqual(metrics["malformed_skipped"], 1)

    def test_order_service_cancellation(self):
        state_entry = {
            "record": {
                "order_id": "ORD-C",
                "current_status": "ORDER_CANCELLED",
                "previous_status": "ORDER_PACKED",
                "last_event_timestamp": "2026-11-13T10:05:00Z",
                "updated_at": "2026-11-13T10:05:01Z",
            }
        }
        events = [
            {
                "record": {
                    "event_id": "e1",
                    "order_id": "ORD-C",
                    "event_type": "ORDER_PLACED",
                    "current_status": "ORDER_PLACED",
                    "event_timestamp": "2026-11-13T10:00:00Z",
                }
            },
            {
                "record": {
                    "event_id": "e2",
                    "order_id": "ORD-C",
                    "event_type": "ORDER_CANCELLED",
                    "previous_status": "ORDER_PACKED",
                    "current_status": "ORDER_CANCELLED",
                    "event_timestamp": "2026-11-13T10:05:00Z",
                }
            },
        ]
        detail = order_service.build_order_detail("ORD-C", state_entry, events)
        self.assertTrue(detail["summary"]["cancelled"])
        delivered = [s for s in detail["stages"] if s["stage"] == "ORDER_DELIVERED"][0]
        self.assertEqual(delivered["status"], "not_reached")

    def test_docker_status_handles_unavailable(self):
        def runner(args, cwd=None, timeout=120, env=None):
            if args[:2] == ["docker", "version"]:
                return CommandResult(False, "docker version", "", "Docker missing", 1)
            return CommandResult(False, "docker inspect", "", "missing", 1)

        service = DockerService(_settings(), _Activity(), runner=runner)
        status = service.status(force=True)
        self.assertFalse(status["available"])
        self.assertFalse(status["required_running"])


if __name__ == "__main__":
    unittest.main()
