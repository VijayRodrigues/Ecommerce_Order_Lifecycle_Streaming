"""Kafka broker and topic metadata monitoring."""

import threading
import time

from datetime import datetime, timezone

from kafka.admin import KafkaAdminClient


def _now_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class KafkaStatusService:
    def __init__(self, settings):
        self.settings = settings
        self._cache = None
        self._cache_at = 0.0
        self._lock = threading.Lock()

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
                < self.settings.kafka_cache_seconds
            ):
                return self._cache

        payload = self._build_status()
        with self._lock:
            self._cache = payload
            self._cache_at = time.monotonic()
        return payload

    def _build_status(self):
        topic_names = [
            self.settings.input_topic,
            self.settings.state_topic,
            self.settings.processed_topic,
            self.settings.exceptions_topic,
        ]

        admin = None
        try:
            admin = KafkaAdminClient(
                bootstrap_servers=self.settings.bootstrap_servers,
                request_timeout_ms=5000,
                api_version_auto_timeout_ms=5000,
                client_id="order-lifecycle-dashboard-status",
            )
            existing = set(admin.list_topics())
            described = admin.describe_topics(topic_names)
            description_by_name = {
                item.get("topic"): item for item in described
            }

            topics = []
            for name in topic_names:
                item = description_by_name.get(name)
                topics.append(
                    {
                        "name": name,
                        "exists": name in existing and item is not None,
                        "partitions": (
                            len(item.get("partitions", []))
                            if item
                            else 0
                        ),
                        "replication_factor": (
                            max(
                                (
                                    len(partition.get("replicas", []))
                                    for partition in item.get("partitions", [])
                                ),
                                default=0,
                            )
                            if item
                            else 0
                        ),
                        "in_sync": (
                            all(
                                bool(partition.get("isr"))
                                for partition in item.get("partitions", [])
                            )
                            if item
                            else False
                        ),
                    }
                )

            return {
                "available": True,
                "bootstrap_servers": self.settings.bootstrap_servers,
                "topics": topics,
                "error": None,
                "checked_at": _now_iso(),
            }
        except Exception as exc:
            return {
                "available": False,
                "bootstrap_servers": self.settings.bootstrap_servers,
                "topics": [
                    {
                        "name": name,
                        "exists": False,
                        "partitions": 0,
                        "replication_factor": 0,
                        "in_sync": False,
                    }
                    for name in topic_names
                ],
                "error": str(exc),
                "checked_at": _now_iso(),
            }
        finally:
            if admin is not None:
                try:
                    admin.close()
                except Exception:
                    pass
