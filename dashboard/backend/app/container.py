"""Application service container."""

import time

from dataclasses import dataclass
from datetime import datetime, timezone

from .config import Settings
from .services.activity import ActivityLog
from .services.docker_service import DockerService
from .services.flink_service import FlinkService
from .services.generator_service import GeneratorService
from .services.infrastructure import InfrastructureStatus
from .services.kafka_monitor import KafkaMonitor
from .services.kafka_status import KafkaStatusService
from .websocket.hub import LiveHub


@dataclass
class AppServices:
    settings: Settings
    activity: ActivityLog
    hub: LiveHub
    monitor: KafkaMonitor
    docker: DockerService
    flink: FlinkService
    generator: GeneratorService
    infrastructure: InfrastructureStatus
    start_background: bool = True

    def health(self):
        now = datetime.now(timezone.utc)
        consumer_health = self.monitor.consumer_health()
        return {
            "status": "ok",
            "service": "order-lifecycle-dashboard",
            "version": self.settings.project_version,
            "time": now.isoformat().replace("+00:00", "Z"),
            "uptime_seconds": round(time.monotonic() - self.started_monotonic, 1),
            "websocket_clients": self.hub.client_count,
            "kafka_consumers": consumer_health,
            "config": self.settings.public_config(),
        }

    started_monotonic: float = 0.0


def create_services(settings=None, start_background=True):
    settings = settings or Settings.from_env()
    activity = ActivityLog(settings.activity_buffer_size)
    hub = LiveHub()
    monitor = KafkaMonitor(settings, hub, activity)
    docker = DockerService(settings, activity)
    flink = FlinkService(settings, activity)
    kafka = KafkaStatusService(settings)
    generator = GeneratorService(settings, activity)
    infrastructure = InfrastructureStatus(docker, flink, kafka, monitor)

    activity.subscribe(
        lambda entry: hub.publish_threadsafe(
            {"type": "activity", "entry": entry}
        )
    )

    return AppServices(
        settings=settings,
        activity=activity,
        hub=hub,
        monitor=monitor,
        docker=docker,
        flink=flink,
        generator=generator,
        infrastructure=infrastructure,
        start_background=start_background,
        started_monotonic=time.monotonic(),
    )
