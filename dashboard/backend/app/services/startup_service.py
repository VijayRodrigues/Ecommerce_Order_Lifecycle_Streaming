"""One-click demo startup orchestration."""

from __future__ import annotations

import threading
import time
from copy import deepcopy
from datetime import datetime, timezone

STEP_DEFS = [
    ("docker", "Checking Docker"),
    ("kafka_start", "Starting Kafka"),
    ("kafka_ready", "Waiting for Kafka"),
    ("topics", "Verifying topics"),
    ("flink_start", "Starting Flink"),
    ("taskmanager", "Checking TaskManager"),
    ("job", "Checking Flink job"),
    ("consumers", "Connecting dashboard consumers"),
    ("verify", "Verifying live monitoring"),
]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class StartupService:
    def __init__(self, settings, activity, hub, docker, flink, kafka, monitor, infrastructure):
        self.settings = settings
        self.activity = activity
        self.hub = hub
        self.docker = docker
        self.flink = flink
        self.kafka = kafka
        self.monitor = monitor
        self.infrastructure = infrastructure
        self._lock = threading.RLock()
        self._thread: threading.Thread | None = None
        self._state = self._initial_state()

    def _initial_state(self):
        return {
            "in_progress": False,
            "demo_ready": False,
            "started_at": None,
            "finished_at": None,
            "current_step": None,
            "message": "Demo environment has not been started yet.",
            "error": None,
            "checks": {
                "docker": "Not Started",
                "kafka": "Not Started",
                "jobmanager": "Not Started",
                "taskmanager": "Not Started",
                "input_topic": "Not Started",
                "output_topics": "Not Started",
                "flink_job": "Not Started",
                "dashboard_consumers": "Not Started",
            },
            "steps": [
                {"id": step_id, "label": label, "status": "pending", "message": "", "detail": None}
                for step_id, label in STEP_DEFS
            ],
        }

    def status(self):
        with self._lock:
            return deepcopy(self._state)

    def _publish(self):
        payload = {"type": "startup", "status": self.status()}
        try:
            self.hub.publish_threadsafe(payload)
        except Exception:
            pass

    def _set(self, **kwargs):
        with self._lock:
            self._state.update(kwargs)
        self._publish()

    def _set_check(self, key, value):
        with self._lock:
            self._state["checks"][key] = value
        self._publish()

    def _set_step(self, step_id, status, message="", detail=None):
        with self._lock:
            for step in self._state["steps"]:
                if step["id"] == step_id:
                    step["status"] = status
                    step["message"] = message
                    step["detail"] = detail
                    break
            self._state["current_step"] = step_id
            if message:
                self._state["message"] = message
        self._publish()

    def start(self, retry=False):
        with self._lock:
            if self._state.get("in_progress"):
                return {
                    "ok": False,
                    "status": "already_running",
                    "message": "Startup is already in progress.",
                    "detail": self.status(),
                }
            self._state = self._initial_state()
            self._state["in_progress"] = True
            self._state["started_at"] = _now_iso()
            self._state["message"] = "Starting demo environment..."
            self._thread = threading.Thread(
                target=self._run, name="dashboard-startup", daemon=True
            )
            self._thread.start()
        self.activity.add(
            "INFO",
            "startup",
            "Demo startup retry started" if retry else "Demo startup started",
        )
        self._publish()
        return {"ok": True, "status": "started", "message": "Demo startup started", "detail": self.status()}

    def refresh_checks(self):
        """Non-destructive status refresh used by UI."""
        infra = self.infrastructure.combined(force=True)
        docker = infra.get("docker", {})
        kafka = infra.get("kafka", {})
        flink = infra.get("flink", {})
        consumers = infra.get("dashboard_consumers", {})
        containers = {c.get("name"): c for c in docker.get("containers", [])}
        job = flink.get("job")
        topics = kafka.get("topics", [])
        input_topic = next((t for t in topics if t.get("name") == self.settings.input_topic), None)
        output_topics = [t for t in topics if t.get("name") != self.settings.input_topic]

        with self._lock:
            checks = self._state["checks"]
            checks["docker"] = "Running" if docker.get("available") else "Failed"
            kafka_running = containers.get(self.settings.kafka_container, {}).get("running")
            checks["kafka"] = "Running" if kafka_running else ("Not Started" if not docker.get("available") else "Not Started")
            jm_running = containers.get(self.settings.flink_jobmanager_container, {}).get("running")
            tm_running = containers.get(self.settings.flink_taskmanager_container, {}).get("running")
            checks["jobmanager"] = "Running" if jm_running else "Not Started"
            checks["taskmanager"] = "Running" if tm_running else "Not Started"
            if kafka.get("available") and input_topic and input_topic.get("exists"):
                checks["input_topic"] = "Connected"
            elif kafka.get("available"):
                checks["input_topic"] = "Needs Attention"
            else:
                checks["input_topic"] = "Not Started" if not kafka_running else "Starting"
            if kafka.get("available") and output_topics and all(t.get("exists") for t in output_topics):
                checks["output_topics"] = "Connected"
            elif kafka.get("available"):
                checks["output_topics"] = "Needs Attention"
            else:
                checks["output_topics"] = "Not Started" if not kafka_running else "Starting"
            if job and job.get("running"):
                checks["flink_job"] = "Running"
            elif flink.get("available"):
                checks["flink_job"] = "Needs Attention"
            else:
                checks["flink_job"] = "Not Started" if not jm_running else "Starting"
            connected = consumers.get("connected", 0)
            total = consumers.get("total", 0)
            if total and connected == total:
                checks["dashboard_consumers"] = "Connected"
            elif connected:
                checks["dashboard_consumers"] = "Needs Attention"
            else:
                checks["dashboard_consumers"] = "Not Started"

            if not self._state.get("in_progress"):
                required = ["docker", "kafka", "jobmanager", "taskmanager", "input_topic", "output_topics", "flink_job", "dashboard_consumers"]
                self._state["demo_ready"] = all(checks[k] in {"Running", "Connected"} for k in required)
                self._state["message"] = "Demo environment ready." if self._state["demo_ready"] else "Demo environment is not fully ready."
        self._publish()
        return self.status()

    def _fail(self, step_id, message, detail=None):
        self._set_step(step_id, "failed", message, detail)
        self.activity.add("ERROR", "startup", message, detail)
        self._set(
            in_progress=False,
            demo_ready=False,
            finished_at=_now_iso(),
            error={"step": step_id, "message": message, "detail": detail},
            message=f"Startup failed: {message}",
        )

    def _wait_until(self, predicate, timeout_s, interval_s=2.0):
        deadline = time.monotonic() + timeout_s
        last = None
        while time.monotonic() < deadline:
            try:
                last = predicate()
            except Exception:
                last = False
            if last:
                return True, last
            time.sleep(interval_s)
        return False, last

    def _run(self):
        try:
            # 1 Docker
            self._set_step("docker", "running", "Checking Docker Engine availability...")
            self._set_check("docker", "Starting")
            docker_status = self.docker.status(force=True)
            if not docker_status.get("available"):
                self._set_check("docker", "Failed")
                return self._fail("docker", "Docker Engine is not available.", docker_status.get("error"))
            self._set_check("docker", "Running")
            self._set_step("docker", "completed", "Docker Engine is available.", docker_status.get("version"))

            # 2 Start Kafka
            self._set_step("kafka_start", "running", "Starting Kafka infrastructure if needed...")
            self._set_check("kafka", "Starting")
            states = {c["name"]: c for c in self.docker.status(force=True).get("containers", [])}
            if states.get(self.settings.kafka_container, {}).get("running"):
                self._set_step("kafka_start", "completed", "Kafka container is already running.")
            else:
                if not self.settings.kafka_compose_file.exists():
                    self._set_check("kafka", "Failed")
                    return self._fail("kafka_start", "Kafka compose file was not found.", str(self.settings.kafka_compose_file))
                result = self.docker._compose_up(self.settings.kafka_compose_file)
                self.docker.invalidate()
                if not result.ok:
                    self._set_check("kafka", "Failed")
                    return self._fail("kafka_start", "Failed to start Kafka infrastructure.", result.as_dict())
                self._set_step("kafka_start", "completed", "Kafka container start requested.")

            # 3 Wait for Kafka
            self._set_step("kafka_ready", "running", "Waiting for Kafka broker to become reachable...")
            ok, last = self._wait_until(lambda: self.kafka.status(force=True).get("available"), timeout_s=90, interval_s=3)
            if not ok:
                self._set_check("kafka", "Failed")
                return self._fail("kafka_ready", "Kafka broker did not become reachable in time.", last)
            self._set_check("kafka", "Running")
            self._set_step("kafka_ready", "completed", "Kafka is running and reachable.")

            # 4 Verify topics
            self._set_step("topics", "running", "Verifying required Kafka topics exist...")
            self._set_check("input_topic", "Starting")
            self._set_check("output_topics", "Starting")
            kstatus = self.kafka.status(force=True)
            topics = {t["name"]: t for t in kstatus.get("topics", [])}
            missing = [name for name, t in topics.items() if not t.get("exists")]
            if missing:
                self._set_check("input_topic", "Needs Attention")
                self._set_check("output_topics", "Needs Attention")
                return self._fail("topics", "Required Kafka topics are missing.", {"missing": missing})
            self._set_check("input_topic", "Connected")
            self._set_check("output_topics", "Connected")
            self._set_step("topics", "completed", "All required Kafka topics are available.")

            # 5 Start Flink
            self._set_step("flink_start", "running", "Starting Flink infrastructure if needed...")
            self._set_check("jobmanager", "Starting")
            self._set_check("taskmanager", "Starting")
            states = {c["name"]: c for c in self.docker.status(force=True).get("containers", [])}
            jm = states.get(self.settings.flink_jobmanager_container, {}).get("running")
            tm = states.get(self.settings.flink_taskmanager_container, {}).get("running")
            if jm and tm:
                self._set_step("flink_start", "completed", "Flink containers are already running.")
            else:
                if not self.settings.flink_compose_file.exists():
                    self._set_check("jobmanager", "Failed")
                    self._set_check("taskmanager", "Failed")
                    return self._fail("flink_start", "Flink compose file was not found.", str(self.settings.flink_compose_file))
                result = self.docker._compose_up(self.settings.flink_compose_file)
                self.docker.invalidate()
                if not result.ok:
                    self._set_check("jobmanager", "Failed")
                    self._set_check("taskmanager", "Failed")
                    return self._fail("flink_start", "Failed to start Flink infrastructure.", result.as_dict())
                self._set_step("flink_start", "completed", "Flink container start requested.")

            # 6 Check TaskManager
            self._set_step("taskmanager", "running", "Waiting for Flink JobManager and TaskManager registration...")
            def flink_ready():
                s = self.flink.status(force=True)
                return bool(s.get("available")) and int(s.get("taskmanagers") or 0) >= 1
            ok, last = self._wait_until(flink_ready, timeout_s=120, interval_s=3)
            if not ok:
                self._set_check("jobmanager", "Failed")
                self._set_check("taskmanager", "Needs Attention")
                return self._fail("taskmanager", "Flink JobManager/TaskManager did not become ready in time.", last)
            self._set_check("jobmanager", "Running")
            self._set_check("taskmanager", "Running")
            self._set_step("taskmanager", "completed", "Flink JobManager is ready and TaskManager is registered.")

            # 7 Check/submit job
            self._set_step("job", "running", "Checking Flink streaming job status...")
            self._set_check("flink_job", "Starting")
            fstatus = self.flink.status(force=True)
            job = fstatus.get("job")
            if job and job.get("running"):
                self.activity.add("INFO", "startup", "Reusing existing running Flink job", job.get("job_id"))
                self._set_step("job", "running", "Flink job is already running. Reusing existing job.")
            else:
                self._set_step("job", "running", "Submitting existing order lifecycle Flink job...")
                submit = self.flink.submit_job()
                if not submit.get("ok") and submit.get("status") not in {"already_running"}:
                    self._set_check("flink_job", "Failed")
                    return self._fail("job", "Flink job submission failed.", submit)
            ok, last = self._wait_until(lambda: (self.flink.status(force=True).get("job") or {}).get("running"), timeout_s=90, interval_s=3)
            if not ok:
                self._set_check("flink_job", "Failed")
                return self._fail("job", "Flink job did not reach RUNNING state.", last)
            job = self.flink.status(force=True).get("job")
            self._set_check("flink_job", "Running")
            self._set_step("job", "completed", f"Flink job is running.", job)

            # 8 Connect consumers
            self._set_step("consumers", "running", "Starting and connecting dashboard Kafka monitors...")
            self._set_check("dashboard_consumers", "Starting")
            self.monitor.start()  # idempotent
            ok, _ = self._wait_until(
                lambda: self.monitor.consumer_health().get("connected") == self.monitor.consumer_health().get("total")
                and self.monitor.consumer_health().get("total", 0) > 0,
                timeout_s=60,
                interval_s=2,
            )
            health = self.monitor.consumer_health()
            if not ok:
                self._set_check("dashboard_consumers", "Needs Attention")
                return self._fail("consumers", "Dashboard Kafka monitors did not fully connect in time.", health)
            self._set_check("dashboard_consumers", "Connected")
            self._set_step("consumers", "completed", f"All {health.get('total')} dashboard Kafka monitors are connected.", health)

            # 9 Verify
            self._set_step("verify", "running", "Verifying live monitoring and pipeline readiness...")
            infra = self.infrastructure.combined(force=True)
            checks = infra.get("pipeline", {}).get("checks", {})
            required = ["docker", "kafka", "topics", "flink", "job", "dashboard_consumers"]
            if not all(checks.get(k) for k in required):
                return self._fail("verify", "Pipeline checks did not all pass after startup.", checks)
            self._set_step("verify", "completed", "Live monitoring is ready.")
            self._set(
                in_progress=False,
                demo_ready=True,
                finished_at=_now_iso(),
                current_step=None,
                error=None,
                message="Demo environment ready.",
            )
            self.activity.add("INFO", "startup", "Demo environment ready")
        except Exception as exc:
            current = "verify"
            with self._lock:
                current = self._state.get("current_step") or "verify"
            self._fail(current, f"Unexpected startup error: {exc}", str(exc))
