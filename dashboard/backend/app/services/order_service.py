"""Order lifecycle aggregation from state and processed records."""

from datetime import datetime, timezone


LIFECYCLE = [
    "ORDER_PLACED",
    "ORDER_CONFIRMED",
    "ORDER_PACKED",
    "ORDER_SHIPPED",
    "OUT_FOR_DELIVERY",
    "ORDER_DELIVERED",
]

STAGE_LABELS = {
    "ORDER_PLACED": "Order placed",
    "ORDER_CONFIRMED": "Confirmed",
    "ORDER_PACKED": "Packed",
    "ORDER_SHIPPED": "Shipped",
    "OUT_FOR_DELIVERY": "Out for delivery",
    "ORDER_DELIVERED": "Delivered",
    "ORDER_CANCELLED": "Cancelled",
}

TERMINAL_STATUSES = {"ORDER_DELIVERED", "ORDER_CANCELLED"}


def parse_timestamp(value):
    if not value or not isinstance(value, str):
        return datetime.min.replace(tzinfo=timezone.utc)
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return datetime.min.replace(tzinfo=timezone.utc)


def _record(entry):
    return entry.get("record", {}) if entry else {}


def _sort_entries(entries):
    return sorted(
        entries,
        key=lambda entry: (
            parse_timestamp(_record(entry).get("event_timestamp")),
            parse_timestamp(_record(entry).get("processed_at")),
        ),
    )


def _index_stage_events(entries):
    stage_events = {}
    cancellation = None
    for entry in _sort_entries(entries):
        value = _record(entry)
        status = value.get("current_status") or value.get("event_type")
        if status == "ORDER_CANCELLED":
            cancellation = entry
        if status in LIFECYCLE:
            stage_events[status] = entry
    return stage_events, cancellation


def _state_stage(state_record):
    status = state_record.get("current_status")
    if status in LIFECYCLE:
        return status
    return None


def _summary(order_id, state_record, events, current_status):
    latest_event = events[-1] if events else {}
    latest_value = _record(latest_event)
    return {
        "order_id": order_id,
        "current_status": (
            current_status
            or state_record.get("current_status")
            or latest_value.get("current_status")
        ),
        "previous_status": (
            state_record.get("previous_status")
            if state_record
            else latest_value.get("previous_status")
        ),
        "last_event_type": (
            state_record.get("last_event_type")
            or latest_value.get("event_type")
        ),
        "last_event_timestamp": (
            state_record.get("last_event_timestamp")
            or latest_value.get("event_timestamp")
        ),
        "updated_at": (
            state_record.get("updated_at")
            or latest_value.get("processed_at")
        ),
        "total_delivery_duration_seconds": (
            state_record.get("total_delivery_duration_seconds")
            if state_record
            else latest_value.get("total_delivery_duration_seconds")
        ),
        "total_delivery_duration_minutes": (
            state_record.get("total_delivery_duration_minutes")
            if state_record
            else latest_value.get("total_delivery_duration_minutes")
        ),
        "stage_duration_seconds": (
            state_record.get("stage_duration_seconds")
            if state_record
            else latest_value.get("stage_duration_seconds")
        ),
        "processed_event_count": len(events),
    }


def build_order_detail(order_id, state_entry, event_entries):
    state_record = _record(state_entry)
    events = _sort_entries(event_entries or [])
    stage_events, cancellation = _index_stage_events(events)

    current_status = (
        state_record.get("current_status")
        or (
            _record(events[-1]).get("current_status")
            if events
            else None
        )
    )
    cancelled = (
        current_status == "ORDER_CANCELLED"
        or cancellation is not None
    )
    if cancelled:
        current_status = "ORDER_CANCELLED"

    if not state_record and not events:
        return {
            "found": False,
            "order_id": order_id,
            "message": "No retained state or processed event was found.",
            "stages": [],
            "summary": None,
        }

    current_index = (
        LIFECYCLE.index(_state_stage(state_record))
        if _state_stage(state_record)
        else -1
    )
    stages = []
    for index, stage in enumerate(LIFECYCLE):
        stage_entry = stage_events.get(stage)
        stage_value = _record(stage_entry)
        has_state_only = (
            not stage_entry
            and _state_stage(state_record) == stage
        )

        status = "pending"
        timestamp = None
        duration = None
        source = None
        event_id = None

        if stage_entry:
            status = "completed"
            timestamp = stage_value.get("event_timestamp")
            duration = stage_value.get("stage_duration_seconds")
            source = "processed"
            event_id = stage_value.get("event_id")
        elif has_state_only:
            status = "completed"
            timestamp = state_record.get("last_event_timestamp")
            duration = state_record.get("stage_duration_seconds")
            source = "state"
            event_id = state_record.get("last_event_id")

        if cancelled:
            if stage_entry:
                status = "completed"
            elif index > current_index and stage != current_status:
                status = "not_reached"
            elif stage == "ORDER_DELIVERED":
                status = "not_reached"
            elif not stage_entry and index < current_index:
                status = "unknown"
            elif not stage_entry:
                status = "not_reached"
        elif not stage_entry and not has_state_only:
            if current_index >= 0 and index < current_index:
                status = "unknown"
            elif current_status == stage:
                status = "unknown"
            elif current_index >= 0 and index > current_index:
                status = "pending"
            elif current_status is None:
                status = "unknown"

        stages.append(
            {
                "stage": stage,
                "label": STAGE_LABELS.get(stage, stage),
                "index": index,
                "status": status,
                "timestamp": timestamp,
                "stage_duration_seconds": duration,
                "source": source,
                "event_id": event_id,
            }
        )

    cancellation_payload = None
    if cancelled:
        cancel_value = _record(cancellation)
        cancellation_payload = {
            "timestamp": (
                cancel_value.get("event_timestamp")
                or state_record.get("last_event_timestamp")
            ),
            "event_id": (
                cancel_value.get("event_id")
                or state_record.get("last_event_id")
            ),
            "label": "Order cancelled",
        }

    summary = _summary(order_id, state_record, events, current_status)
    summary["cancelled"] = cancelled
    summary["delivered"] = current_status == "ORDER_DELIVERED"

    return {
        "found": True,
        "order_id": order_id,
        "summary": summary,
        "stages": stages,
        "cancellation": cancellation_payload,
        "processed_events": list(reversed(events)),
        "state": state_entry,
        "completed_stages": sum(
            1 for stage in stages if stage["status"] == "completed"
        ),
        "note": (
            "Processed events provide lifecycle history. The compacted state "
            "topic only provides the latest known order state."
        ),
    }


def build_order_summaries(
    state_entries,
    order_events,
    limit=100,
    search=None,
    active_only=False,
):
    limit = max(1, min(int(limit), 500))
    search_value = (search or "").strip().lower()
    rows = []

    for order_id, state_entry in state_entries.items():
        events = order_events.get(order_id, [])
        record = _record(state_entry)
        latest = _record(events[-1]) if events else {}
        current_status = (
            record.get("current_status")
            or latest.get("current_status")
        )
        rows.append(
            {
                "order_id": order_id,
                "current_status": current_status,
                "previous_status": (
                    record.get("previous_status")
                    or latest.get("previous_status")
                ),
                "last_event_timestamp": (
                    record.get("last_event_timestamp")
                    or latest.get("event_timestamp")
                ),
                "updated_at": (
                    record.get("updated_at")
                    or latest.get("processed_at")
                ),
                "total_delivery_duration_seconds": (
                    record.get("total_delivery_duration_seconds")
                ),
                "processed_event_count": len(events),
                "active": current_status not in TERMINAL_STATUSES,
            }
        )

    known = {row["order_id"] for row in rows}
    for order_id, events in order_events.items():
        if order_id in known or not events:
            continue
        ordered = _sort_entries(events)
        latest = _record(ordered[-1])
        current_status = latest.get("current_status")
        rows.append(
            {
                "order_id": order_id,
                "current_status": current_status,
                "previous_status": latest.get("previous_status"),
                "last_event_timestamp": latest.get("event_timestamp"),
                "updated_at": latest.get("processed_at"),
                "total_delivery_duration_seconds": latest.get(
                    "total_delivery_duration_seconds"
                ),
                "processed_event_count": len(events),
                "active": current_status not in TERMINAL_STATUSES,
            }
        )

    if search_value:
        rows = [
            row
            for row in rows
            if search_value in row["order_id"].lower()
        ]
    if active_only:
        rows = [row for row in rows if row["active"]]

    rows.sort(
        key=lambda row: parse_timestamp(
            row.get("updated_at") or row.get("last_event_timestamp")
        ),
        reverse=True,
    )
    return rows[:limit]
