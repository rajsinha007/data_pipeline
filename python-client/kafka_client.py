#!/usr/bin/env python3
"""Read topic2 from the beginning using mutual TLS and SASL authentication."""

import argparse
import getpass
import os
import sys
import uuid
from pathlib import Path


def required_env(name):
    value = os.environ.get(name, "").strip()
    if not value:
        raise ValueError(f"Set the {name} environment variable.")
    return value


def certificate_path(name):
    path = Path(required_env(name)).expanduser().resolve()
    if not path.is_file():
        raise ValueError(f"{name} must point to an existing PEM file: {path}")
    return str(path)


def client_config(timeout):
    mechanism = os.environ.get("KAFKA_SASL_MECHANISM", "SCRAM-SHA-512").upper()
    if mechanism not in {"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"}:
        raise ValueError("KAFKA_SASL_MECHANISM must be PLAIN, SCRAM-SHA-256 or SCRAM-SHA-512.")

    config = {
        "bootstrap.servers": required_env("KAFKA_BOOTSTRAP_SERVERS"),
        "client.id": "python-mtls-sasl-client",
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": mechanism,
        "sasl.username": required_env("KAFKA_SASL_USERNAME"),
        "ssl.ca.location": certificate_path("KAFKA_SSL_CA_LOCATION"),
        "ssl.certificate.location": certificate_path("KAFKA_SSL_CERTIFICATE_LOCATION"),
        "ssl.key.location": certificate_path("KAFKA_SSL_KEY_LOCATION"),
        "enable.ssl.certificate.verification": True,
        "ssl.endpoint.identification.algorithm": "https",
        "socket.timeout.ms": int(timeout * 1000),
    }
    password = os.environ.get("KAFKA_SASL_PASSWORD")
    if password is None:
        if not sys.stdin.isatty():
            raise ValueError("Set KAFKA_SASL_PASSWORD when running non-interactively.")
        password = getpass.getpass("Kafka SASL password: ")
    if not password:
        raise ValueError("Kafka SASL password must not be empty.")
    config["sasl.password"] = password
    key_password = os.environ.get("KAFKA_SSL_KEY_PASSWORD")
    if key_password:
        config["ssl.key.password"] = key_password
    return config


def consume_topic(config, topic, timeout, max_messages=None):
    from confluent_kafka import Consumer, KafkaException, OFFSET_BEGINNING, TopicPartition

    consumer = Consumer({
        **config,
        "group.id": f"python-topic-reader-{uuid.uuid4().hex}",
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "auto.offset.reset": "earliest",
        "allow.auto.create.topics": False,
    })
    try:
        metadata = consumer.list_topics(timeout=timeout)
        topic_metadata = metadata.topics.get(topic)
        if topic_metadata is None:
            raise ValueError(f"Topic {topic!r} does not exist or is not visible to this user.")
        if topic_metadata.error is not None:
            raise KafkaException(topic_metadata.error)
        if not topic_metadata.partitions:
            raise ValueError(f"Topic {topic!r} has no available partitions.")
        # Explicit assignment ignores any previously committed consumer offsets.
        consumer.assign([
            TopicPartition(topic, partition, OFFSET_BEGINNING)
            for partition in sorted(topic_metadata.partitions)
        ])
        print(f"Connected to Kafka cluster: {metadata.cluster_id or '(unknown ID)'}", flush=True)
        print(f"Reading {topic} from the earliest retained offset in every partition. Ctrl+C to stop.", flush=True)
        count = 0
        while max_messages is None or count < max_messages:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error() is not None:
                raise KafkaException(message.error())
            value = message.value()
            text = "<tombstone>" if value is None else value.decode("utf-8", errors="replace")
            print(f"{message.topic()} [{message.partition()}] offset {message.offset()}: {text}", flush=True)
            count += 1
    finally:
        consumer.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--topic", default="test2", help="Topic to read or publish to (default: topic2).")
    parser.add_argument("--message", help="Publish this UTF-8 message instead of consuming.")
    parser.add_argument("--check", action="store_true", help="Check connectivity without consuming or publishing.")
    parser.add_argument("--max-messages", type=int, help="Stop after reading this many messages; otherwise keep listening.")
    parser.add_argument("--timeout", type=float, default=30, help="Operation timeout in seconds (default: 30).")
    args = parser.parse_args()
    if not args.topic.strip():
        parser.error("--topic must not be empty")
    if args.check and args.message is not None:
        parser.error("--check and --message cannot be combined")
    if args.max_messages is not None:
        if args.max_messages <= 0:
            parser.error("--max-messages must be greater than zero")
        if args.check or args.message is not None:
            parser.error("--max-messages is only valid when consuming")
    if not 0 < args.timeout <= 3600:
        parser.error("--timeout must be greater than zero and at most 3600 seconds")

    try:
        from confluent_kafka import KafkaException, Producer
    except ImportError:
        print("Missing dependency: install confluent-kafka from the supplied requirements file.", file=sys.stderr)
        return 1

    try:
        config = client_config(args.timeout)
        if not args.check and args.message is None:
            consume_topic(config, args.topic, args.timeout, args.max_messages)
            return 0
        producer = Producer({
            **config,
            "message.timeout.ms": int(args.timeout * 1000),
            "enable.idempotence": True,
        })
        metadata = producer.list_topics(timeout=args.timeout)
        print(f"Connected to Kafka cluster: {metadata.cluster_id or '(unknown ID)'}")
        for broker_id, broker in sorted(metadata.brokers.items()):
            print(f"Broker {broker_id}: {broker.host}:{broker.port}")

        if args.message is not None:
            delivery_errors = []

            def on_delivery(error, message):
                if error is not None:
                    delivery_errors.append(str(error))
                else:
                    print(f"Delivered to {message.topic()} [{message.partition()}] offset {message.offset()}")

            producer.produce(args.topic, value=args.message.encode("utf-8"), on_delivery=on_delivery)
            remaining = producer.flush(args.timeout)
            if delivery_errors:
                raise RuntimeError("Delivery failed: " + "; ".join(delivery_errors))
            if remaining:
                raise RuntimeError("Delivery timed out; message delivery is unconfirmed.")
        return 0
    except (ValueError, OSError, KafkaException, RuntimeError, BufferError) as error:
        print(f"Kafka operation failed: {error}", file=sys.stderr)
        return 1
    except (KeyboardInterrupt, EOFError):
        print("Cancelled.", file=sys.stderr)
        return 130


if __name__ == "__main__":
    sys.exit(main())