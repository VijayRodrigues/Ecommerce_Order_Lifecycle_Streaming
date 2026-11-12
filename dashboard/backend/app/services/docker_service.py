"""Allowlisted Docker infrastructure monitoring and controls."""

import threading
import time

from datetime import datetime, timezone

from .processes import run_command


def _now_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class DockerService:
    def __init__(self, settings, activity=None, runner=None):
        self.settings = settings
        self.activity = activity
        self.runner = runner or run_command
        self._cache = None
        self._cache_at = 0.0
        self._lock = threading.Lock()
        self.required_containers = (
            settings.kafka_container,
            settings.flink_jobmanager_container,
            settings.flink_taskmanager_container,
        )

    def _log(self, level, message, detail=None):
        if self.activity:
            self.activity.add(level, "docker", message, detail)

    def invalidate(self):
        with self._lock:
            self._cache = None
            self._cache_at = 0.0

    def status(self, force=False):
        with self._lock:
            if (
                not force
                and self._cache is not None
                and time.monotonic() - self._cache_at
                < self.settings.docker_cache_seconds
            ):
                return self._cache

        payload = self._build_status()
        with self._lock:
            self._cache = payload
            self._cache_at = time.monotonic()
        return payload

    def _build_status(self):
        version = self.runner(
            ["docker", "version", "--format", "{{.Server.Version}}"],
            timeout=12,
        )
        available = version.ok and bool(version.stdout.strip())
        containers = []

        for name in self.required_containers:
            containers.append(self._container_state(name))

        return {
            "available": available,
            "version": version.stdout.strip() if available else None,
            "error": (
                None
                if available
                else (version.stderr.strip() or "Docker Engine unavailable")
            ),
            "containers": containers,
            "required_running": all(
                item["running"] for item in containers
            ),
            "compose_files": {
                "kafka": {
                    "path": str(self.settings.kafka_compose_file),
                    "exists": self.settings.kafka_compose_file.exists(),
                },
                "flink": {
                    "path": str(self.settings.flink_compose_file),
                    "exists": self.settings.flink_compose_file.exists(),
                },
            },
            "checked_at": _now_iso(),
        }

    def _container_state(self, name):
        result = self.runner(
            [
                "docker",
                "inspect",
                "--format",
                (
                    "{{.State.Status}}|{{.State.StartedAt}}|"
                    "{{if .State.Health}}{{.State.Health.Status}}"
                    "{{else}}-{{end}}"
                ),
                name,
            ],
            timeout=12,
        )

        if not result.ok:
            return {
                "name": name,
                "exists": False,
                "running": False,
                "status": "missing",
                "started_at": None,
                "health": None,
            }

        parts = result.stdout.strip().split("|")
        status = parts[0] if parts else "unknown"
        started_at = parts[1] if len(parts) > 1 else None
        health = parts[2] if len(parts) > 2 else None

        return {
            "name": name,
            "exists": True,
            "running": status == "running",
            "status": status,
            "started_at": started_at,
            "health": None if health == "-" else health,
        }

    def _compose_up(self, compose_file):
        result = self.runner(
            ["docker", "compose", "-f", str(compose_file), "up", "-d"],
            cwd=compose_file.parent,
            timeout=self.settings.command_timeout_seconds,
        )
        return result

    def start_infrastructure(self):
        current = self.status(force=True)
        if not current["available"]:
            return {
                "ok": False,
                "status": "docker_unavailable",
                "message": current["error"]
                or "Docker Engine is not available",
                "detail": current,
            }

        states = {item["name"]: item for item in current["containers"]}
        if all(item["running"] for item in states.values()):
            return {
                "ok": True,
                "status": "already_running",
                "message": "Kafka and Flink infrastructure are already running",
                "detail": current,
            }

        outputs = []

        if not states[self.settings.kafka_container]["running"]:
            if not self.settings.kafka_compose_file.exists():
                return {
                    "ok": False,
                    "status": "missing_compose_file",
                    "message": "Kafka compose file was not found",
                    "detail": str(self.settings.kafka_compose_file),
                }
            result = self._compose_up(self.settings.kafka_compose_file)
            outputs.append(result.as_dict())
            if not result.ok:
                self._log("ERROR", "Kafka infrastructure start failed", result.stderr)
                self.invalidate()
                return {
                    "ok": False,
                    "status": "start_failed",
                    "message": "Kafka infrastructure failed to start",
                    "detail": outputs,
                }

        flink_running = (
            states[self.settings.flink_jobmanager_container]["running"]
            and states[self.settings.flink_taskmanager_container]["running"]
        )
        if not flink_running:
            if not self.settings.flink_compose_file.exists():
                return {
                    "ok": False,
                    "status": "missing_compose_file",
                    "message": "Flink compose file was not found",
                    "detail": str(self.settings.flink_compose_file),
                }
            result = self._compose_up(self.settings.flink_compose_file)
            outputs.append(result.as_dict())
            if not result.ok:
                self._log("ERROR", "Flink infrastructure start failed", result.stderr)
                self.invalidate()
                return {
                    "ok": False,
                    "status": "start_failed",
                    "message": "Flink infrastructure failed to start",
                    "detail": outputs,
                }

        self.invalidate()
        final = self.status(force=True)
        self._log("INFO", "Infrastructure start completed")
        return {
            "ok": final["required_running"],
            "status": (
                "started" if final["required_running"] else "start_incomplete"
            ),
            "message": (
                "Kafka and Flink infrastructure are running"
                if final["required_running"]
                else "Infrastructure start did not reach a healthy state"
            ),
            "detail": {
                "commands": outputs,
                "status": final,
            },
        }

    def stop_infrastructure(self):
        current = self.status(force=True)
        if not current["available"]:
            return {
                "ok": False,
                "status": "docker_unavailable",
                "message": current["error"]
                or "Docker Engine is not available",
                "detail": current,
            }

        running = [
            item["name"]
            for item in current["containers"]
            if item["running"]
        ]
        if not running:
            return {
                "ok": True,
                "status": "already_stopped",
                "message": "Kafka and Flink infrastructure are already stopped",
                "detail": current,
            }

        result = self.runner(
            ["docker", "stop", *running],
            timeout=self.settings.command_timeout_seconds,
        )
        self.invalidate()
        final = self.status(force=True)
        self._log(
            "INFO" if result.ok else "ERROR",
            "Infrastructure stop completed" if result.ok else "Infrastructure stop failed",
            result.stderr,
        )
        return {
            "ok": result.ok and not final["required_running"],
            "status": "stopped" if result.ok else "stop_failed",
            "message": (
                "Kafka and Flink containers were stopped without removing data"
                if result.ok
                else "Infrastructure stop failed"
            ),
            "detail": {
                "command": result.as_dict(),
                "status": final,
            },
        }
