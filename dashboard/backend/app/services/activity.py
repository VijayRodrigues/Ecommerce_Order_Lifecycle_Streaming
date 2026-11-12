"""Bounded, thread-safe dashboard activity log."""

import logging
import threading

from collections import deque
from datetime import datetime, timezone


logger = logging.getLogger("order_lifecycle.dashboard")


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class ActivityLog:
    def __init__(self, capacity=250):
        self._entries = deque(maxlen=capacity)
        self._lock = threading.Lock()
        self._subscribers = []

    def subscribe(self, callback):
        with self._lock:
            self._subscribers.append(callback)

    def add(self, level, source, message, detail=None):
        entry = {
            "timestamp": utc_now_iso(),
            "level": level.upper(),
            "source": source,
            "message": str(message)[:500],
        }

        if detail:
            entry["detail"] = str(detail)[:2000]

        with self._lock:
            self._entries.appendleft(entry)
            subscribers = list(self._subscribers)

        log_method = {
            "ERROR": logger.error,
            "WARNING": logger.warning,
            "INFO": logger.info,
            "DEBUG": logger.debug,
        }.get(entry["level"], logger.info)
        log_method("%s | %s | %s", source, message, detail or "")

        for callback in subscribers:
            try:
                callback(dict(entry))
            except Exception:
                logger.exception("Activity subscriber failed")

        return dict(entry)

    def recent(self, limit=100):
        limit = max(1, min(int(limit), 250))
        with self._lock:
            return [dict(entry) for entry in list(self._entries)[:limit]]
