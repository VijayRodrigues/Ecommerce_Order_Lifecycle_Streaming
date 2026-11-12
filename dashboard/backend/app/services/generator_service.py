"""Managed subprocess wrapper for the existing Kafka event producer."""

import os
import subprocess
import sys
import threading

from collections import deque
from datetime import datetime, timezone


SCENARIOS = {
    "normal_flow": (
        "Simulates a customer order progressing through the normal delivery "
        "journey."
    ),
    "duplicate_events": (
        "Sends repeated events to demonstrate duplicate detection."
    ),
    "late_events": (
        "Sends events that arrive outside the configured event-time tolerance."
    ),
    "out_of_order_events": (
        "Sends lifecycle events in a different arrival order."
    ),
    "invalid_events": (
        "Sends malformed or invalid lifecycle events."
    ),
    "continuous": (
        "Continuously generates new customer order journeys."
    ),
}
SCENARIO_NAMES = tuple(SCENARIOS)


def _now_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def build_generator_command(python_executable, scenario, interval=2.0):
    if scenario not in SCENARIOS:
        raise ValueError(f"Unsupported generator scenario: {scenario}")
    if not 0 < float(interval) <= 60:
        raise ValueError("Generator interval must be between 0 and 60 seconds")

    return [
        str(python_executable),
        "-u",
        "-m",
        "order_lifecycle.kafka.producer",
        "--scenario",
        scenario,
        "--interval",
        str(float(interval)),
    ]


class GeneratorService:
    def __init__(self, settings, activity):
        self.settings = settings
        self.activity = activity
        self._lock = threading.RLock()
        self._process = None
        self._reader_thread = None
        self._watcher_thread = None
        self._logs = deque(maxlen=250)
        self._status = self._empty_status()

    def _empty_status(self):
        return {
            "running": False,
            "mode": None,
            "scenario": None,
            "started_at": None,
            "finished_at": None,
            "last_execution_at": None,
            "events_generated": 0,
            "errors": 0,
            "exit_code": None,
            "last_line": None,
            "command": None,
        }

    def python_executable(self):
        if self.settings.venv_python.exists():
            return self.settings.venv_python
        return sys.executable

    def _spawn(self, command):
        creationflags = 0
        if os.name == "nt":
            creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
        return subprocess.Popen(
            command,
            cwd=str(self.settings.repo_root),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
            shell=False,
            creationflags=creationflags,
            env={
                **os.environ,
                "PYTHONUNBUFFERED": "1",
            },
        )

    def run_once(self, scenario, interval=2.0):
        if scenario == "continuous":
            return {
                "ok": False,
                "status": "invalid_request",
                "message": "Use the continuous start control for continuous mode",
                "detail": None,
            }
        return self._start(scenario, interval, mode="once")

    def start_continuous(self, interval=2.0):
        return self._start("continuous", interval, mode="continuous")

    def _start(self, scenario, interval, mode):
        try:
            command = build_generator_command(
                self.python_executable(),
                scenario,
                interval,
            )
        except ValueError as exc:
            return {
                "ok": False,
                "status": "invalid_request",
                "message": str(exc),
                "detail": None,
            }

        with self._lock:
            if self._process is not None and self._process.poll() is None:
                return {
                    "ok": False,
                    "status": "already_running",
                    "message": "A generator process is already running",
                    "detail": self._status,
                }

            self._logs.clear()
            self._status = {
                **self._empty_status(),
                "running": True,
                "mode": mode,
                "scenario": scenario,
                "started_at": _now_iso(),
                "last_execution_at": _now_iso(),
                "command": " ".join(command),
            }

            try:
                process = self._spawn(command)
            except (OSError, ValueError) as exc:
                self._status.update(
                    {
                        "running": False,
                        "finished_at": _now_iso(),
                        "errors": 1,
                        "last_line": str(exc),
                    }
                )
                self._append_log("ERROR", f"Generator failed to start: {exc}")
                self.activity.add(
                    "ERROR",
                    "generator",
                    "Generator failed to start",
                    str(exc),
                )
                return {
                    "ok": False,
                    "status": "start_failed",
                    "message": "Generator process failed to start",
                    "detail": {"error": str(exc), "command": command},
                }

            self._process = process
            self._reader_thread = threading.Thread(
                target=self._read_output,
                args=(process,),
                name=f"dashboard-generator-output-{scenario}",
                daemon=True,
            )
            self._watcher_thread = threading.Thread(
                target=self._watch,
                args=(process, mode),
                name=f"dashboard-generator-watch-{scenario}",
                daemon=True,
            )
            self._reader_thread.start()
            self._watcher_thread.start()

        self._append_log("INFO", f"Started {scenario} generator ({mode})")
        self.activity.add(
            "INFO",
            "generator",
            f"Started {scenario} generator",
            " ".join(command),
        )
        return {
            "ok": True,
            "status": "started",
            "message": f"Generator started: {scenario}",
            "detail": self.status(),
        }

    def _append_log(self, level, message):
        with self._lock:
            self._logs.appendleft(
                {
                    "timestamp": _now_iso(),
                    "level": level,
                    "message": str(message)[:1000],
                }
            )

    def _read_output(self, process):
        try:
            for line in iter(process.stdout.readline, ""):
                if not line:
                    break
                text = line.rstrip()
                if not text:
                    continue
                level = "ERROR" if (
                    "ERROR" in text
                    or "Traceback" in text
                    or "Exception" in text
                ) else "INFO"
                with self._lock:
                    self._status["last_line"] = text[:1000]
                    if "Published " in text:
                        self._status["events_generated"] += 1
                    if level == "ERROR":
                        self._status["errors"] += 1
                self._append_log(level, text)
        except Exception as exc:
            self._append_log("ERROR", f"Generator output reader failed: {exc}")
        finally:
            try:
                process.stdout.close()
            except Exception:
                pass

    def _watch(self, process, mode):
        timeout = (
            self.settings.generator_max_finite_seconds
            if mode == "once"
            else None
        )
        timed_out = False
        try:
            exit_code = process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            timed_out = True
            process.terminate()
            try:
                exit_code = process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                exit_code = process.wait()
        finally:
            finished_at = _now_iso()

        with self._lock:
            if self._process is process:
                self._process = None
            self._status.update(
                {
                    "running": False,
                    "finished_at": finished_at,
                    "last_execution_at": finished_at,
                    "exit_code": exit_code,
                }
            )
            scenario = self._status.get("scenario")

        if timed_out:
            self._append_log(
                "ERROR",
                f"Generator exceeded {timeout} seconds and was stopped",
            )
            self.activity.add(
                "WARNING",
                "generator",
                "Generator stopped after reaching its time limit",
                scenario,
            )
        elif exit_code == 0:
            self._append_log("INFO", f"Generator finished with exit code 0")
            self.activity.add(
                "INFO",
                "generator",
                "Generator finished",
                scenario,
            )
        else:
            self._append_log(
                "ERROR",
                f"Generator exited with code {exit_code}",
            )
            self.activity.add(
                "ERROR",
                "generator",
                "Generator exited with a non-zero code",
                f"scenario={scenario}, exit_code={exit_code}",
            )

    def stop(self):
        with self._lock:
            process = self._process
            if process is None or process.poll() is not None:
                return {
                    "ok": True,
                    "status": "not_running",
                    "message": "No generator process is running",
                    "detail": self.status(),
                }
            scenario = self._status.get("scenario")

        self.activity.add(
            "INFO",
            "generator",
            "Stopping generator",
            scenario,
        )
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                pass

        self._append_log("INFO", "Generator stop requested")
        return {
            "ok": True,
            "status": "stopped",
            "message": "Generator process stopped",
            "detail": self.status(),
        }

    def status(self):
        with self._lock:
            payload = dict(self._status)
            if self._process is not None:
                payload["running"] = self._process.poll() is None
            payload["available_scenarios"] = {
                name: description
                for name, description in SCENARIOS.items()
            }
            payload["log_count"] = len(self._logs)
            return payload

    def logs(self, limit=100):
        limit = max(1, min(int(limit), 250))
        with self._lock:
            return [dict(item) for item in list(self._logs)[:limit]]
