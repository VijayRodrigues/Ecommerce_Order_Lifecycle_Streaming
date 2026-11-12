"""Combined infrastructure and end-to-end pipeline status."""

from datetime import datetime, timezone


def _now_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class InfrastructureStatus:
    def __init__(self, docker, flink, kafka, monitor):
        self.docker = docker
        self.flink = flink
        self.kafka = kafka
        self.monitor = monitor

    def combined(self, force=False):
        docker_status = self.docker.status(force=force)
        kafka_status = self.kafka.status(force=force)
        flink_status = self.flink.status(force=force)
        consumer_health = self.monitor.consumer_health()

        topics_ok = (
            kafka_status.get("available")
            and all(
                topic.get("exists") for topic in kafka_status.get("topics", [])
            )
        )
        job = flink_status.get("job")
        job_running = bool(job and job.get("running"))
        consumers_ok = (
            consumer_health["connected"] == consumer_health["total"]
        )
        docker_ok = docker_status.get("available")

        checks = {
            "docker": bool(docker_ok),
            "kafka": bool(kafka_status.get("available")),
            "topics": bool(topics_ok),
            "flink": bool(flink_status.get("available")),
            "job": job_running,
            "dashboard_consumers": consumers_ok,
        }

        healthy_count = sum(1 for value in checks.values() if value)
        if healthy_count == len(checks):
            pipeline_status = "healthy"
            pipeline_label = "Pipeline healthy"
        elif healthy_count == 0:
            pipeline_status = "offline"
            pipeline_label = "Pipeline unavailable"
        else:
            pipeline_status = "degraded"
            pipeline_label = "Pipeline degraded"

        return {
            "checked_at": _now_iso(),
            "pipeline": {
                "status": pipeline_status,
                "label": pipeline_label,
                "healthy": pipeline_status == "healthy",
                "checks": checks,
            },
            "docker": docker_status,
            "kafka": kafka_status,
            "flink": flink_status,
            "dashboard_consumers": consumer_health,
            "last_checkpoint": (
                (flink_status.get("checkpoints") or {})
                .get("latest", {})
                .get("completed")
            ),
        }
