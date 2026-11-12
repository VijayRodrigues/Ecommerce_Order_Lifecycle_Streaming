"""Consume accepted lifecycle events from Kafka."""

import os

from order_lifecycle.consumers.common import (
    DEFAULT_PROCESSED_TOPIC,
    build_arg_parser,
    format_processed_record,
    run_consumer,
    topic_from_env,
    validate_args,
)


DEFAULT_GROUP_ID = "order-lifecycle-processed-consumer"


def main():
    parser = build_arg_parser(
        "Consume and display accepted order lifecycle events.",
        os.getenv("KAFKA_PROCESSED_CONSUMER_GROUP", DEFAULT_GROUP_ID),
    )
    args = parser.parse_args()
    validate_args(parser, args)

    topic = topic_from_env(
        "KAFKA_PROCESSED_TOPIC",
        DEFAULT_PROCESSED_TOPIC,
    )

    return run_consumer(
        topic=topic,
        group_id=args.group_id,
        bootstrap_servers=args.bootstrap_servers,
        format_record=format_processed_record,
        from_beginning=args.from_beginning,
        max_messages=args.max_messages,
        idle_timeout=args.idle_timeout,
    )


if __name__ == "__main__":
    raise SystemExit(main())
