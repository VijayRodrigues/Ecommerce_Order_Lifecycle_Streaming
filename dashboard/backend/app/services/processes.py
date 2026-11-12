"""Controlled subprocess execution for Docker, Flink and generators."""

import os
import subprocess

from dataclasses import dataclass


BLOCKED_DOCKER_SUBCOMMANDS = {
    "down",
    "kill",
    "prune",
    "rm",
    "rmi",
    "system",
    "volume",
}

ALLOWED_DOCKER_SUBCOMMANDS = {
    "version",
    "inspect",
    "compose",
    "stop",
    "start",
    "exec",
    "cp",
    "ps",
}


@dataclass
class CommandResult:
    ok: bool
    command: str
    stdout: str
    stderr: str
    returncode: int | None
    timed_out: bool = False

    def as_dict(self):
        return {
            "ok": self.ok,
            "command": self.command,
            "stdout": self.stdout,
            "stderr": self.stderr,
            "returncode": self.returncode,
            "timed_out": self.timed_out,
        }


def validate_docker_command(args):
    if not isinstance(args, (list, tuple)) or not args:
        raise ValueError("Docker command must be a non-empty argument list")

    if any(not isinstance(part, str) for part in args):
        raise ValueError("Docker command arguments must be strings")

    if args[0].lower() != "docker":
        raise ValueError("Only the docker executable is permitted")

    subcommand = args[1].lower() if len(args) > 1 else ""
    if subcommand in BLOCKED_DOCKER_SUBCOMMANDS:
        raise ValueError(f"Docker subcommand is not permitted: {subcommand}")

    if subcommand not in ALLOWED_DOCKER_SUBCOMMANDS:
        raise ValueError(f"Docker subcommand is not allowlisted: {subcommand}")

    if subcommand == "compose":
        compose_tokens = {part.lower() for part in args[2:]}
        if "down" in compose_tokens or "rm" in compose_tokens:
            raise ValueError("Destructive docker compose operations are blocked")

    return list(args)


def sanitize_output(value, limit=6000):
    if value is None:
        return ""
    text = str(value).replace("\x00", "")
    return text[-limit:]


def run_command(args, cwd=None, timeout=120, env=None):
    is_docker = bool(args) and str(args[0]).lower() == "docker"
    if is_docker:
        args = validate_docker_command(args)
    else:
        args = [str(part) for part in args]

    creationflags = 0
    if os.name == "nt":
        creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)

    try:
        completed = subprocess.run(
            args,
            cwd=str(cwd) if cwd else None,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout,
            shell=False,
            env=env,
            creationflags=creationflags,
        )
        return CommandResult(
            ok=completed.returncode == 0,
            command=" ".join(args),
            stdout=sanitize_output(completed.stdout),
            stderr=sanitize_output(completed.stderr),
            returncode=completed.returncode,
        )
    except subprocess.TimeoutExpired as exc:
        return CommandResult(
            ok=False,
            command=" ".join(args),
            stdout=sanitize_output(exc.stdout),
            stderr=sanitize_output(exc.stderr),
            returncode=None,
            timed_out=True,
        )
    except FileNotFoundError as exc:
        return CommandResult(
            ok=False,
            command=" ".join(args),
            stdout="",
            stderr=str(exc),
            returncode=None,
        )
    except OSError as exc:
        return CommandResult(
            ok=False,
            command=" ".join(args),
            stdout="",
            stderr=str(exc),
            returncode=None,
        )
