import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from dashboard.backend.app.services.startup_service import StartupService


def _settings():
    return SimpleNamespace(
        kafka_container="kafka_kraft_docker",
        flink_jobmanager_container="flink-jobmanager",
        flink_taskmanager_container="flink-taskmanager",
        input_topic="ecommerce.order-lifecycle.events",
        state_topic="ecommerce.order-lifecycle.state",
        processed_topic="ecommerce.order-lifecycle.processed",
        exceptions_topic="ecommerce.order-lifecycle.exceptions",
        kafka_compose_file=Path("F:/DockerLab/messaging/kafka_kraft_docker/compose.yaml"),
        flink_compose_file=Path("F:/DockerLab/processing/flink_docker/docker-compose.yml"),
    )


class _Cmd:
    def __init__(self, ok=True):
        self.ok = ok
        self.stderr = "" if ok else "fail"

    def as_dict(self):
        return {"ok": self.ok, "stderr": self.stderr}


class StartupServiceTests(unittest.TestCase):
    def _service(self, docker=None, flink=None, kafka=None, monitor=None, infra=None):
        activity = MagicMock()
        hub = MagicMock()
        docker = docker or MagicMock()
        flink = flink or MagicMock()
        kafka = kafka or MagicMock()
        monitor = monitor or MagicMock()
        infra = infra or MagicMock()
        return StartupService(_settings(), activity, hub, docker, flink, kafka, monitor, infra)

    def test_refresh_checks_ready(self):
        monitor = MagicMock()
        monitor.consumer_health.return_value = {"connected": 3, "total": 3, "consumers": []}
        infra = MagicMock()
        infra.combined.return_value = {
            "pipeline": {"checks": {"docker": True, "kafka": True, "topics": True, "flink": True, "job": True, "dashboard_consumers": True}},
            "docker": {"available": True, "containers": [
                {"name": "kafka_kraft_docker", "running": True},
                {"name": "flink-jobmanager", "running": True},
                {"name": "flink-taskmanager", "running": True},
            ]},
            "kafka": {"available": True, "topics": [
                {"name": "ecommerce.order-lifecycle.events", "exists": True},
                {"name": "ecommerce.order-lifecycle.state", "exists": True},
                {"name": "ecommerce.order-lifecycle.processed", "exists": True},
                {"name": "ecommerce.order-lifecycle.exceptions", "exists": True},
            ]},
            "flink": {"available": True, "job": {"running": True, "job_id": "a" * 32}},
            "dashboard_consumers": {"connected": 3, "total": 3},
        }
        s = self._service(monitor=monitor, infra=infra)
        st = s.refresh_checks()
        self.assertTrue(st["demo_ready"])
        self.assertEqual(st["checks"]["docker"], "Running")
        self.assertEqual(st["checks"]["dashboard_consumers"], "Connected")
        self.assertEqual(st["checks"]["flink_job"], "Running")

    def test_refresh_checks_not_started(self):
        infra = MagicMock()
        infra.combined.return_value = {
            "pipeline": {"checks": {}},
            "docker": {"available": False, "containers": []},
            "kafka": {"available": False, "topics": []},
            "flink": {"available": False, "job": None},
            "dashboard_consumers": {"connected": 0, "total": 3},
        }
        s = self._service(infra=infra)
        st = s.refresh_checks()
        self.assertFalse(st["demo_ready"])
        self.assertEqual(st["checks"]["docker"], "Failed")
        self.assertEqual(st["checks"]["dashboard_consumers"], "Not Started")

    def test_duplicate_start_prevention(self):
        s = self._service()
        with s._lock:
            s._state["in_progress"] = True
        res = s.start()
        self.assertFalse(res["ok"])
        self.assertEqual(res["status"], "already_running")

    @patch("dashboard.backend.app.services.startup_service.time.sleep", return_value=None)
    def test_run_reuses_running_services_and_job(self, _sleep):
        docker = MagicMock()
        docker.status.side_effect = lambda force=True: {"available": True, "containers": [
            {"name": "kafka_kraft_docker", "running": True},
            {"name": "flink-jobmanager", "running": True},
            {"name": "flink-taskmanager", "running": True},
        ]}
        docker._compose_up = MagicMock()
        kafka = MagicMock()
        kafka.status.return_value = {"available": True, "topics": [
            {"name": "ecommerce.order-lifecycle.events", "exists": True},
            {"name": "ecommerce.order-lifecycle.state", "exists": True},
            {"name": "ecommerce.order-lifecycle.processed", "exists": True},
            {"name": "ecommerce.order-lifecycle.exceptions", "exists": True},
        ]}
        flink = MagicMock()
        flink.status.return_value = {"available": True, "taskmanagers": 1, "job": {"running": True, "job_id": "a" * 32}}
        flink.submit_job = MagicMock()
        monitor = MagicMock()
        monitor.consumer_health.return_value = {"connected": 3, "total": 3}
        monitor.start = MagicMock()
        infra = MagicMock()
        infra.combined.return_value = {"pipeline": {"checks": {"docker": True, "kafka": True, "topics": True, "flink": True, "job": True, "dashboard_consumers": True}}}
        s = self._service(docker=docker, flink=flink, kafka=kafka, monitor=monitor, infra=infra)
        s._run()
        st = s.status()
        self.assertTrue(st["demo_ready"])
        docker._compose_up.assert_not_called()
        flink.submit_job.assert_not_called()
        monitor.start.assert_called()

    @patch("dashboard.backend.app.services.startup_service.time.sleep", return_value=None)
    def test_run_fails_when_docker_unavailable(self, _sleep):
        docker = MagicMock()
        docker.status.return_value = {"available": False, "error": "no docker", "containers": []}
        s = self._service(docker=docker)
        s._run()
        st = s.status()
        self.assertFalse(st["demo_ready"])
        self.assertEqual(st["error"]["step"], "docker")
        self.assertEqual(st["checks"]["docker"], "Failed")

    @patch("dashboard.backend.app.services.startup_service.time.sleep", return_value=None)
    def test_run_fails_when_kafka_not_reachable(self, _sleep):
        docker = MagicMock()
        docker.status.return_value = {"available": True, "containers": [{"name": "kafka_kraft_docker", "running": True}, {"name": "flink-jobmanager", "running": True}, {"name": "flink-taskmanager", "running": True}]}
        kafka = MagicMock()
        kafka.status.return_value = {"available": False, "topics": []}
        s = self._service(docker=docker, kafka=kafka)
        s._wait_until = lambda predicate, timeout_s, interval_s=2.0: (False, False)
        s._run()
        st = s.status()
        self.assertEqual(st["error"]["step"], "kafka_ready")
        self.assertEqual(st["checks"]["kafka"], "Failed")

    @patch("dashboard.backend.app.services.startup_service.time.sleep", return_value=None)
    def test_run_fails_when_flink_unavailable(self, _sleep):
        docker = MagicMock()
        docker.status.return_value = {"available": True, "containers": [{"name": "kafka_kraft_docker", "running": True}, {"name": "flink-jobmanager", "running": True}, {"name": "flink-taskmanager", "running": True}]}
        kafka = MagicMock()
        kafka.status.return_value = {"available": True, "topics": [
            {"name": "ecommerce.order-lifecycle.events", "exists": True},
            {"name": "ecommerce.order-lifecycle.state", "exists": True},
            {"name": "ecommerce.order-lifecycle.processed", "exists": True},
            {"name": "ecommerce.order-lifecycle.exceptions", "exists": True},
        ]}
        flink = MagicMock()
        flink.status.return_value = {"available": False, "taskmanagers": 0, "job": None}
        s = self._service(docker=docker, kafka=kafka, flink=flink)
        calls = {"n": 0}
        def wait(pred, timeout_s, interval_s=2.0):
            calls["n"] += 1
            return (True, True) if calls["n"] == 1 else (False, False)
        s._wait_until = wait
        s._run()
        st = s.status()
        self.assertEqual(st["error"]["step"], "taskmanager")

    @patch("dashboard.backend.app.services.startup_service.time.sleep", return_value=None)
    def test_retry_after_partial_failure(self, _sleep):
        docker = MagicMock()
        docker.status.return_value = {"available": False, "error": "no docker", "containers": []}
        s = self._service(docker=docker)
        s._run()
        self.assertIsNotNone(s.status()["error"])
        res = s.start(retry=True)
        self.assertTrue(res["ok"])
        self.assertEqual(res["status"], "started")


if __name__ == "__main__":
    unittest.main()
