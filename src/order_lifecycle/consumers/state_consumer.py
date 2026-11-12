"""Display the latest retained order state from Kafka.

The state topic is compacted. Kafka retains the latest value for each order
key, not every historical state transition. Reading from the beginning shows
the retained latest values rather than a complete event history.
"""

import os

from order_lifecycle.consumers.common import (
    DEFAULT_STATE_TOPIC,
    build_arg_parser,
    format_state_record,
    run_consumer,
    topic_from_env,
    validate_args,
)


DEFAULT_GROUP_ID = "order-lifecycle-state-consumer"


def main():
    parser = build_arg_parser(
        "Consume and display ecommerce order state records.",
        os.getenv("KAFKA_STATE_CONSUMER_GROUP", DEFAULT_GROUP_ID),
    )
    args = parser.parse_args()
    validate_args(parser, args)

    topic = topic_from_env("KAFKA_STATE_TOPIC", DEFAULT_STATE_TOPIC)

    print(
        "State topic note: Kafka compaction keeps the latest value per key, "
        "not a complete event history."
    )

    return run_consumer(
        topic=topic,
        group_id=args.group_id,
        bootstrap_servers=args.bootstrap_servers,
        format_record=format_state_record,
        from_beginning=args.from_beginning,
        max_messages=args.max_messages,
        idle_timeout=args.idle_timeout,
    )


if __name__ == "__main__":
    raise SystemExit(main())
