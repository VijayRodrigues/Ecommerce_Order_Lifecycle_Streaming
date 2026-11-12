"""Background Kafka monitoring for dashboard state and live streams."""

import threading
import time

from collections import OrderedDict, deque
from datetime import datetime, timezone

from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaError

from order_lifecycle.consumers.common import (
    decode_key,
    parse_record_value,
)

from . import order_service


TERMINAL_STATUSES = {"ORDER_DELIVERED", "ORDER_CANCELLED"}
RECONNECT_DELAY_SECONDS = 3.0
GROUP_ASSIGNMENT_TIMEOUT_SECONDS = 3.0


def _now_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class KafkaMonitor:
    def __init__(self, settings, hub, activity):
        self.settings = settings
        self.hub = hub
        self.activity = activity
        self._lock = threading.RLock()
        self._stop_event = threading.Event()
        self._threads = []
        self._started_at = None
        self._last_event_at = None
        self._states = OrderedDict()
        self._order_events = OrderedDict()
        self._processed = deque(maxlen=settings.processed_buffer_size)
        self._exceptions = deque(maxlen=settings.exceptions_buffer_size)
        self._seen_event_ids = OrderedDict()
        self._seen_event_id_limit = 10000
        self._next_offsets = {}
        self._consumer_status = {}
        self._counters = {
            "processed": 0,
            "state_updates": 0,
            "exceptions": 0,
            "delay_alerts": 0,
            "duplicate_processed_ignored": 0,
            "malformed_skipped": 0,
        }
        self._specs = (
            {
                "kind": "state",
                "topic": settings.state_topic,
                "group_id": settings.state_group_id,
            },
            {
                "kind": "processed",
                "topic": settings.processed_topic,
                "group_id": settings.processed_group_id,
            },
            {
                "kind": "exceptions",
                "topic": settings.exceptions_topic,
                "group_id": settings.exceptions_group_id,
            },
        )
        for spec in self._specs:
            self._consumer_status[spec["kind"]] = {
                "kind": spec["kind"],
                "topic": spec["topic"],
                "group_id": spec["group_id"],
                "state": "stopped",
                "connected": False,
                "mode": None,
                "messages": 0,
                "malformed": 0,
                "last_message_at": None,
                "last_error": None,
            }

    def start(self):
        if self._threads:
            return
        self._started_at = _now_iso()
        self._stop_event.clear()
        for spec in self._specs:
            thread = threading.Thread(
                target=self._consume_loop,
                args=(spec,),
                name=f"dashboard-kafka-{spec['kind']}",
                daemon=True,
            )
            thread.start()
            self._threads.append(thread)
        self.activity.add(
            "INFO",
            "kafka",
            "Dashboard Kafka monitors started",
            ", ".join(spec["topic"] for spec in self._specs),
        )

    def stop(self):
        self._stop_event.set()
        for thread in self._threads:
            thread.join(timeout=5)
        self._threads = []
        with self._lock:
            for status in self._consumer_status.values():
                status["connected"] = False
                status["state"] = "stopped"
        self.activity.add("INFO", "kafka", "Dashboard Kafka monitors stopped")

    def _set_status(self, kind, **values):
        with self._lock:
            self._consumer_status[kind].update(values)

    def _create_consumer(self, spec):
        group_consumer = None
        try:
            group_consumer = KafkaConsumer(
                spec["topic"],
                bootstrap_servers=self.settings.bootstrap_servers,
                group_id=spec["group_id"],
                auto_offset_reset=(
                    "earliest"
                    if self.settings.kafka_start_from_beginning
                    else "latest"
                ),
                enable_auto_commit=True,
                client_id=f"dashboard-{spec['kind']}",
            )
            deadline = (
                time.monotonic() + GROUP_ASSIGNMENT_TIMEOUT_SECONDS
            )
            while time.monotonic() < deadline:
                group_consumer.poll(timeout_ms=500)
                if group_consumer.assignment():
                    return group_consumer, "group"
        except (KafkaError, OSError):
            pass

        if group_consumer is not None:
            try:
                group_consumer.close()
            except Exception:
                pass

        consumer = KafkaConsumer(
            bootstrap_servers=self.settings.bootstrap_servers,
            auto_offset_reset=(
                "earliest"
                if self.settings.kafka_start_from_beginning
                else "latest"
            ),
            enable_auto_commit=False,
            client_id=f"dashboard-{spec['kind']}-manual",
        )

        partitions = consumer.partitions_for_topic(spec["topic"])
        if not partitions:
            consumer.close()
            raise KafkaError(
                f"Topic does not exist or has no partitions: {spec['topic']}"
            )

        assignments = [
            TopicPartition(spec["topic"], partition)
            for partition in sorted(partitions)
        ]
        consumer.assign(assignments)

        with self._lock:
            saved_offsets = dict(
                self._next_offsets.get(spec["kind"], {})
            )

        for topic_partition in assignments:
            saved = saved_offsets.get(topic_partition)
            if saved is not None:
                consumer.seek(topic_partition, saved)
            elif self.settings.kafka_start_from_beginning:
                consumer.seek_to_beginning(topic_partition)
            else:
                consumer.seek_to_end(topic_partition)

        return consumer, "manual"

    def _consume_loop(self, spec):
        kind = spec["kind"]
        while not self._stop_event.is_set():
            self._set_status(
                kind,
                state="connecting",
                connected=False,
                last_error=None,
            )
            consumer = None
            try:
                consumer, mode = self._create_consumer(spec)
                self._set_status(
                    kind,
                    state="connected",
                    connected=True,
                    mode=mode,
                    last_error=None,
                )
                self.activity.add(
                    "INFO",
                    "kafka",
                    f"Connected to {spec['topic']} using {mode} assignment",
                    f"group={spec['group_id']}",
                )

                while not self._stop_event.is_set():
                    batches = consumer.poll(timeout_ms=1000)
                    for topic_partition, records in batches.items():
                        for record in records:
                            try:
                                self._handle_record(spec, record)
                            except Exception as exc:
                                self._set_status(
                                    kind,
                                    last_error=str(exc),
                                )
                                self.activity.add(
                                    "ERROR",
                                    "kafka",
                                    f"Failed to process {spec['topic']} record",
                                    str(exc),
                                )
                            finally:
                                if mode == "manual":
                                    with self._lock:
                                        offsets = self._next_offsets.setdefault(
                                            kind,
                                            {},
                                        )
                                        offsets[topic_partition] = record.offset + 1
            except (KafkaError, OSError) as exc:
                self._set_status(
                    kind,
                    state="reconnecting",
                    connected=False,
                    last_error=str(exc),
                )
                self.activity.add(
                    "WARNING",
                    "kafka",
                    f"Kafka monitor reconnecting for {spec['topic']}",
                    str(exc),
                )
            except Exception as exc:
                self._set_status(
                    kind,
                    state="error",
                    connected=False,
                    last_error=str(exc),
                )
                self.activity.add(
                    "ERROR",
                    "kafka",
                    f"Kafka monitor failed for {spec['topic']}",
                    str(exc),
                )
            finally:
                if consumer is not None:
                    try:
                        consumer.close()
                    except Exception:
                        pass

            if not self._stop_event.is_set():
                self._stop_event.wait(RECONNECT_DELAY_SECONDS)

    def _handle_record(self, spec, record):
        try:
            value = parse_record_value(record.value)
        except ValueError as exc:
            with self._lock:
                self._counters["malformed_skipped"] += 1
                self._consumer_status[spec["kind"]]["malformed"] += 1
            self.activity.add(
                "WARNING",
                "kafka",
                f"Malformed dashboard message in {spec['topic']}",
                str(exc),
            )
            return

        key = decode_key(record.key)
        entry = {
            "kind": spec["kind"],
            "record": value,
            "kafka": {
                "topic": record.topic,
                "partition": record.partition,
                "offset": record.offset,
                "key": key,
            },
            "received_at": _now_iso(),
        }

        if spec["kind"] == "processed":
            entry = self._ingest_processed(entry)
        elif spec["kind"] == "state":
            entry = self._ingest_state(entry)
        else:
            entry = self._ingest_exception(entry)

        with self._lock:
            status = self._consumer_status[spec["kind"]]
            status["messages"] += 1
            status["last_message_at"] = entry["received_at"]
        self._last_event_at = entry["received_at"]

        if entry is not None:
            self.hub.publish_threadsafe(
                {
                    "type": spec["kind"],
                    "entry": entry,
                }
            )

    def _register_event_id(self, event_id):
        if not event_id:
            return True
        with self._lock:
            if event_id in self._seen_event_ids:
                return False
            self._seen_event_ids[event_id] = True
            while len(self._seen_event_ids) > self._seen_event_id_limit:
                self._seen_event_ids.popitem(last=False)
            return True

    def _ingest_processed(self, entry):
        value = entry["record"]
        event_id = value.get("event_id")
        if not self._register_event_id(event_id):
            with self._lock:
                self._counters["duplicate_processed_ignored"] += 1
            return None

        order_id = value.get("order_id") or entry["kafka"]["key"]
        with self._lock:
            self._processed.appendleft(entry)
            self._counters["processed"] += 1
            if order_id and order_id != "-":
                events = self._order_events.setdefault(
                    order_id,
                    deque(maxlen=self.settings.order_event_buffer_size),
                )
                events.append(entry)
                self._order_events.move_to_end(order_id)
                while len(self._order_events) > self.settings.state_buffer_size:
                    self._order_events.popitem(last=False)
        return entry

    def _ingest_state(self, entry):
        value = entry["record"]
        order_id = value.get("order_id") or entry["kafka"]["key"]
        with self._lock:
            self._counters["state_updates"] += 1
            if order_id and order_id != "-":
                self._states[order_id] = entry
                self._states.move_to_end(order_id)
                while len(self._states) > self.settings.state_buffer_size:
                    self._states.popitem(last=False)
        return entry

    def _ingest_exception(self, entry):
        value = entry["record"]
        is_delay = bool(
            value.get("alert_type")
            or value.get("exception_category") == "DELAY_ALERTS"
        )
        entry["is_delay_alert"] = is_delay
        with self._lock:
            self._exceptions.appendleft(entry)
            self._counters["exceptions"] += 1
            if is_delay:
                self._counters["delay_alerts"] += 1
        return entry

    def metrics(self):
        with self._lock:
            states = dict(self._states)
            counters = dict(self._counters)
            statuses = [
                dict(status) for status in self._consumer_status.values()
            ]
            processed_size = len(self._processed)
            exceptions_size = len(self._exceptions)
            state_size = len(self._states)

        active_orders = sum(
            1
            for entry in states.values()
            if entry["record"].get("current_status")
            not in TERMINAL_STATUSES
        )
        total_events = counters["processed"] + counters["exceptions"]
        return {
            "started_at": self._started_at,
            "last_event_at": self._last_event_at,
            "total_events": total_events,
            "processed_events": counters["processed"],
            "state_updates": counters["state_updates"],
            "exceptions": counters["exceptions"],
            "delay_alerts": counters["delay_alerts"],
            "duplicate_processed_ignored": counters[
                "duplicate_processed_ignored"
            ],
            "malformed_skipped": counters["malformed_skipped"],
            "active_orders": active_orders,
            "known_orders": state_size,
            "buffers": {
                "processed": processed_size,
                "exceptions": exceptions_size,
                "states": state_size,
            },
            "consumers": statuses,
            "counters_note": (
                "Observed by this dashboard since backend startup, including "
                "retained Kafka records loaded at startup."
            ),
        }

    def snapshot(self, limit=100):
        limit = max(1, min(int(limit), 500))
        with self._lock:
            processed = list(self._processed)[:limit]
            states = list(reversed(list(self._states.values())))[:limit]
            exceptions = list(self._exceptions)[:limit]
            counters = dict(self._counters)
        return {
            "processed": processed,
            "states": states,
            "exceptions": exceptions,
            "counters": counters,
        }

    def recent_processed(self, limit=100):
        return self.snapshot(limit)["processed"]

    def recent_exceptions(self, limit=100):
        return self.snapshot(limit)["exceptions"]

    def recent_states(self, limit=100):
        return self.snapshot(limit)["states"]

    def orders(self, limit=100, search=None, active_only=False):
        with self._lock:
            state_entries = dict(self._states)
            order_events = {
                order_id: list(entries)
                for order_id, entries in self._order_events.items()
            }
        return order_service.build_order_summaries(
            state_entries,
            order_events,
            limit=limit,
            search=search,
            active_only=active_only,
        )

    def order_detail(self, order_id):
        with self._lock:
            state_entry = self._states.get(order_id)
            event_entries = list(self._order_events.get(order_id, []))
        return order_service.build_order_detail(
            order_id,
            state_entry,
            event_entries,
        )

    def consumer_health(self):
        with self._lock:
            statuses = [
                dict(status) for status in self._consumer_status.values()
            ]
        return {
            "consumers": statuses,
            "connected": sum(
                1 for status in statuses if status["connected"]
            ),
            "total": len(statuses),
        }
