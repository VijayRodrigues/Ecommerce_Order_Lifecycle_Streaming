
import argparse
import json
import random
import time
import uuid

from datetime import datetime, timedelta, timezone


SOURCES = {
    "ORDER_PLACED": "order_service",
    "ORDER_CONFIRMED": "order_service",
    "ORDER_PACKED": "warehouse",
    "ORDER_SHIPPED": "warehouse",
    "OUT_FOR_DELIVERY": "delivery_partner",
    "ORDER_DELIVERED": "delivery_partner",
    "ORDER_CANCELLED": "order_service"
}

LIFECYCLE = [
    "ORDER_PLACED",
    "ORDER_CONFIRMED",
    "ORDER_PACKED",
    "ORDER_SHIPPED",
    "OUT_FOR_DELIVERY",
    "ORDER_DELIVERED"
]


def create_event(order_id, event_type, timestamp):
    return {
        "schema_version": "1.0",
        "event_id": str(uuid.uuid4()),
        "order_id": order_id,
        "event_type": event_type,
        "event_timestamp": timestamp.isoformat().replace("+00:00", "Z"),
        "source": SOURCES[event_type]
    }


def generate_scenario(scenario, start_time=None, step=None):
    order_id = f"ORD-{uuid.uuid4().hex[:8].upper()}"

    if start_time is None:
        start_time = (
            datetime.now(timezone.utc).replace(microsecond=0)
            - timedelta(minutes=50)
        )

    if step is None:
        step = timedelta(minutes=10)

    events = [
        create_event(
            order_id,
            event_type,
            start_time + step * index
        )
        for index, event_type in enumerate(LIFECYCLE)
    ]

    if scenario == "normal_flow":
        return events

    if scenario == "duplicate_events":
        return events[:3] + [events[2].copy()] + events[3:]

    if scenario == "late_events":
        return [events[0], events[1], events[3],
                events[4], events[5], events[2]]

    if scenario == "out_of_order_events":
        out_of_order_packed = create_event(
            order_id,
            "ORDER_PACKED",
            start_time + step * 1.2
        )

        return [events[0], out_of_order_packed, events[1],
                events[3], events[4], events[5]]

    if scenario == "invalid_events":
        invalid_event = create_event(
            order_id,
            "ORDER_DELIVERED",
            start_time + step
        )

        watermark_advance_event = create_event(
            order_id,
            "ORDER_CONFIRMED",
            start_time + step * 2
        )

        missing_id_event = create_event(
            order_id,
            "ORDER_CONFIRMED",
            start_time + step * 2
        )
        del missing_id_event["order_id"]

        return [events[0], invalid_event,
                watermark_advance_event, missing_id_event]

    raise ValueError(f"Unknown scenario: {scenario}")


def main():
    parser = argparse.ArgumentParser(
        description="E-commerce Order Lifecycle Event Generator"
    )

    parser.add_argument(
        "--scenario",
        choices=[
            "normal_flow",
            "duplicate_events",
            "late_events",
            "out_of_order_events",
            "invalid_events",
            "continuous"
        ],
        default="normal_flow"
    )

    parser.add_argument(
        "--interval",
        type=float,
        default=1.0,
        help="Seconds between generated events"
    )

    args = parser.parse_args()

    if args.scenario == "continuous":
        step = timedelta(seconds=args.interval)
        cursor = (
            datetime.now(timezone.utc).replace(microsecond=0)
            - step * (len(LIFECYCLE) - 1)
        )
    else:
        step = None
        cursor = None

    while True:
        if args.scenario == "continuous":
            events = generate_scenario(
                "normal_flow",
                start_time=cursor,
                step=step
            )
            cursor += step * len(events)
        else:
            events = generate_scenario(args.scenario)

        for index, event in enumerate(events):
            if args.scenario == "late_events" and index == len(events) - 1:
                time.sleep(max(args.interval, 1.0))

            print(json.dumps(event), flush=True)

            if args.scenario == "continuous":
                time.sleep(args.interval)

        if args.scenario != "continuous":
            break


if __name__ == "__main__":
    main()
