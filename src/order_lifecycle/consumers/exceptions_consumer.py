"""Consume malformed events, duplicates, late events, invalid transitions,
and delay alerts from Kafka.
"""

import os

from order_lifecycle.consumers.common import (
    DEFAULT_EXCEPTIONS_TOPIC,
    build_arg_parser,
    format_exception_record,
    run_consumer,
    topic_from_env,
    validate_args,
)


DEFAULT_GROUP_ID = "order-lifecycle-exceptions-consumer"


def main():
    parser = build_arg_parser(
        "Consume and display order lifecycle exceptions and delay alerts.",
        os.getenv("KAFKA_EXCEPTIONS_CONSUMER_GROUP", DEFAULT_GROUP_ID),
    )
    args = parser.parse_args()
    validate_args(parser, args)

    topic = topic_from_env(
        "KAFKA_EXCEPTIONS_TOPIC",
        DEFAULT_EXCEPTIONS_TOPIC,
    )

    return run_consumer(
        topic=topic,
        group_id=args.group_id,
        bootstrap_servers=args.bootstrap_servers,
        format_record=format_exception_record,
        from_beginning=args.from_beginning,
        max_messages=args.max_messages,
        idle_timeout=args.idle_timeout,
    )


if __name__ == "__main__":
    raise SystemExit(main())
