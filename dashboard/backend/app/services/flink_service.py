"""Flink REST monitoring and safe job lifecycle controls."""

import json
import os
import re
import threading
import time
import urllib.error
import urllib.request

from datetime import datetime, timezone

from .processes import run_command, sanitize_output


JOB_ID_PATTERN = re.compile(r"^[a-fA-F0-9]{32}$")
ACTIVE_JOB_STATES = {
    "INITIALIZING",
    "CREATED",
    "RUNNING",
    "FAILING",
    "CANCELLING",
    "RESTARTING",
    "RECONCILING",
}


def _now_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class FlinkService:
    def __init__(self, settings, activity=None, runner=None):
        self.settings = settings
        self.activity = activity
        self.runner = runner or run_command
        self._cache = None
        self._cache_at = 0.0
        self._lock = threading.Lock()

    def _log(self, level, message, detail=None):
        if self.activity:
            self.activity.add(level, "flink", message, detail)

    def invalidate(self):
        with self._lock:
            self._cache = None
            self._cache_at = 0.0

    def _request(self, path, method="GET", timeout=5):
        url = f"{self.settings.flink_rest_url}{path}"
        request = urllib.request.Request(
            url,
            method=method,
            headers={"Accept": "application/json"},
        )
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = response.read().decode("utf-8", errors="replace")
            if not body:
                return None
            return json.loads(body)

    def _base_status(self):
        try:
            overview = self._request("/overview")
            jobs = self._request("/jobs/overview")
        except (urllib.error.URLError, OSError, ValueError) as exc:
            return {
                "available": False,
                "rest_url": self.settings.flink_rest_url,
                "error": str(exc),
                "checked_at": _now_iso(),
            }

        job_rows = jobs.get("jobs", []) if isinstance(jobs, dict) else []
        project_jobs = [
            job
            for job in job_rows
            if job.get("name") == self.settings.flink_job_name
        ]
        active_job = next(
            (
                job
                for job in project_jobs
                if job.get("state") in ACTIVE_JOB_STATES
            ),
            None,
        )
        if active_job is None and project_jobs:
            active_job = project_jobs[0]

        checkpoint = None
        if active_job:
            checkpoint = self._checkpoint_summary(active_job.get("jid"))

        return {
            "available": True,
            "rest_url": self.settings.flink_rest_url,
            "flink_version": overview.get("flink-version"),
            "taskmanagers": overview.get("taskmanagers"),
            "slots_total": overview.get("slots-total"),
            "slots_available": overview.get("slots-available"),
            "jobs_running": overview.get("jobs-running"),
            "jobs_finished": overview.get("jobs-finished"),
            "jobs_failed": overview.get("jobs-failed"),
            "job": self._normalize_job(active_job),
            "checkpoints": checkpoint,
            "error": None,
            "checked_at": _now_iso(),
        }

    def _normalize_job(self, job):
        if not job:
            return None
        state = job.get("state")
        duration_ms = job.get("duration")
        return {
            "job_id": job.get("jid"),
            "name": job.get("name"),
            "state": state,
            "running": state in ACTIVE_JOB_STATES,
            "start_time": job.get("start-time"),
            "end_time": job.get("end-time"),
            "duration_ms": duration_ms,
            "duration_seconds": (
                round(duration_ms / 1000, 1)
                if isinstance(duration_ms, (int, float))
                else None
            ),
            "tasks": job.get("tasks", {}),
            "last_modification": job.get("last-modification"),
        }

    def _checkpoint_summary(self, job_id):
        try:
            payload = self._request(
                f"/jobs/{job_id}/checkpoints",
                timeout=8,
            )
        except (urllib.error.URLError, OSError, ValueError) as exc:
            return {
                "available": False,
                "error": str(exc),
                "latest": None,
                "counts": None,
                "history": [],
            }

        counts = payload.get("counts", {})
        latest = payload.get("latest", {}) or {}
        completed = latest.get("completed")
        savepoint = latest.get("savepoint")
        failed = latest.get("failed")
        restored = latest.get("restored")

        return {
            "available": True,
            "counts": counts,
            "latest": {
                "completed": self._normalize_checkpoint(completed),
                "savepoint": self._normalize_checkpoint(savepoint),
                "failed": self._normalize_checkpoint(failed),
                "restored": self._normalize_checkpoint(restored),
            },
            "history": [
                self._normalize_checkpoint(item)
                for item in payload.get("history", [])[:12]
                if item
            ],
        }

    def _normalize_checkpoint(self, checkpoint):
        if not checkpoint:
            return None
        return {
            "id": checkpoint.get("id"),
            "status": checkpoint.get("status"),
            "checkpoint_type": checkpoint.get("checkpoint_type"),
            "is_savepoint": checkpoint.get("is_savepoint"),
            "trigger_timestamp": checkpoint.get("trigger_timestamp"),
            "latest_ack_timestamp": checkpoint.get("latest_ack_timestamp"),
            "checkpointed_size": checkpoint.get("checkpointed_size"),
            "state_size": checkpoint.get("state_size"),
            "end_to_end_duration": checkpoint.get("end_to_end_duration"),
            "num_subtasks": checkpoint.get("num_subtasks"),
            "num_acknowledged_subtasks": checkpoint.get(
                "num_acknowledged_subtasks"
            ),
            "external_path": checkpoint.get("external_path"),
            "discarded": checkpoint.get("discarded"),
        }

    def status(self, force=False):
        with self._lock:
            if (
                not force
                and self._cache is not None
                and time.monotonic() - self._cache_at
                < self.settings.flink_cache_seconds
            ):
                return self._cache

        payload = self._base_status()
        with self._lock:
            self._cache = payload
            self._cache_at = time.monotonic()
        return payload

    def _flink_directory(self):
        return self.settings.repo_root / "src" / "order_lifecycle" / "flink"

    def _processing_rules(self):
        return self.settings.repo_root / "config" / "processing_rules.json"

    def submit_job(self):
        current = self.status(force=True)
        if not current.get("available"):
            return {
                "ok": False,
                "status": "flink_unavailable",
                "message": current.get("error") or "Flink REST is unavailable",
                "detail": current,
            }

        job = current.get("job")
        if job and job.get("running"):
            return {
                "ok": True,
                "status": "already_running",
                "message": (
                    f"Flink job is already running: {job.get('job_id')}"
                ),
                "detail": job,
            }

        job_dir = self._flink_directory()
        rules_path = self._processing_rules()
        if not job_dir.exists():
            return {
                "ok": False,
                "status": "missing_job_files",
                "message": "Flink job source directory was not found",
                "detail": str(job_dir),
            }
        if not rules_path.exists():
            return {
                "ok": False,
                "status": "missing_config",
                "message": "Processing rules configuration was not found",
                "detail": str(rules_path),
            }

        commands = [
            (
                [
                    "docker",
                    "exec",
                    self.settings.flink_jobmanager_container,
                    "mkdir",
                    "-p",
                    "/tmp/order-lifecycle/config",
                ],
                30,
            ),
            (
                [
                    "docker",
                    "cp",
                    str(job_dir) + os.sep + ".",
                    (
                        f"{self.settings.flink_jobmanager_container}:"
                        "/tmp/order-lifecycle/"
                    ),
                ],
                60,
            ),
            (
                [
                    "docker",
                    "cp",
                    str(rules_path),
                    (
                        f"{self.settings.flink_jobmanager_container}:"
                        "/tmp/order-lifecycle/config/processing_rules.json"
                    ),
                ],
                60,
            ),
            (
                [
                    "docker",
                    "exec",
                    self.settings.flink_jobmanager_container,
                    "flink",
                    "run",
                    "-d",
                    "-py",
                    "/tmp/order-lifecycle/order_lifecycle_job.py",
                ],
                120,
            ),
        ]

        results = []
        for args, timeout in commands:
            result = self.runner(args, timeout=timeout)
            results.append(result.as_dict())
            if not result.ok:
                self._log(
                    "ERROR",
                    "Flink job submission failed",
                    result.stderr,
                )
                return {
                    "ok": False,
                    "status": "submit_failed",
                    "message": "Flink job submission command failed",
                    "detail": {
                        "commands": results,
                        "stderr": sanitize_output(result.stderr),
                    },
                }

        self.invalidate()
        time.sleep(1.0)
        updated = self.status(force=True)
        self._log("INFO", "Flink job submission command completed")
        return {
            "ok": bool(updated.get("job")),
            "status": "submitted" if updated.get("job") else "submitted_pending",
            "message": (
                "Flink job submitted"
                if updated.get("job")
                else "Submission completed; Flink job is not visible yet"
            ),
            "detail": {
                "commands": results,
                "flink": updated,
            },
        }

    def stop_job(self):
        current = self.status(force=True)
        if not current.get("available"):
            return {
                "ok": False,
                "status": "flink_unavailable",
                "message": current.get("error") or "Flink REST is unavailable",
                "detail": current,
            }

        job = current.get("job")
        if not job or not job.get("running"):
            return {
                "ok": True,
                "status": "not_running",
                "message": "No running project Flink job was found",
                "detail": job,
            }

        job_id = job.get("job_id") or ""
        if not JOB_ID_PATTERN.match(job_id):
            return {
                "ok": False,
                "status": "invalid_job_id",
                "message": "Flink returned an unexpected job identifier",
                "detail": job_id,
            }

        try:
            self._request(
                f"/jobs/{job_id}?mode=cancel",
                method="PATCH",
                timeout=10,
            )
        except (urllib.error.URLError, OSError, ValueError) as exc:
            self._log("ERROR", "Flink job stop request failed", str(exc))
            return {
                "ok": False,
                "status": "stop_failed",
                "message": "Flink job stop request failed",
                "detail": str(exc),
            }

        self.invalidate()
        self._log("INFO", f"Flink job cancel requested: {job_id}")
        time.sleep(1.0)
        updated = self.status(force=True)
        return {
            "ok": True,
            "status": "stop_requested",
            "message": f"Cancellation requested for Flink job {job_id}",
            "detail": updated,
        }
