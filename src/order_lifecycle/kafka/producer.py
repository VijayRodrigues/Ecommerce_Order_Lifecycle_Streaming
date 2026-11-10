
import argparse
import json
import logging
import time

from kafka import KafkaProducer
from order_lifecycle.generator.event_generator import generate_scenario


BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "ecommerce.order-lifecycle.events"


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)


def publish_event(producer, event):
    future = producer.send(
        TOPIC,
        key=event.get("order_id", "INVALID").encode("utf-8"),
        value=event
    )

    result = future.get(timeout=10)

    logger.info(
        "Published %s | Partition: %s | Offset: %s",
        event.get("event_type"),
        result.partition,
        result.offset
    )


def main():
    parser = argparse.ArgumentParser(
        description="E-commerce Order Lifecycle Kafka Producer"
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
        default=2.0,
        help="Seconds between events in continuous mode"
    )

    args = parser.parse_args()

    if args.interval <= 0:
        parser.error("--interval must be greater than zero")

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=None,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        retries=5
    )

    try:
        logger.info("Connected to Kafka at %s", BOOTSTRAP_SERVERS)

        while True:
            scenario = (
                "normal_flow"
                if args.scenario == "continuous"
                else args.scenario
            )

            events = generate_scenario(scenario)

            for event in events:
                publish_event(producer, event)

                if args.scenario == "continuous":
                    time.sleep(args.interval)

            if args.scenario != "continuous":
                break

    except KeyboardInterrupt:
        logger.info("Producer stopped by user")

    finally:
        producer.flush()
        producer.close()
        logger.info("Kafka producer closed")


if __name__ == "__main__":
    main()
