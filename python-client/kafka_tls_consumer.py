#!/usr/bin/env python3
"""Print Kafka records from the beginning using TLS client certificates (no SASL)."""

import argparse
import math
import os
import sys
import uuid
from pathlib import Path


CERT_DIR = Path(__file__).resolve().parent.parent / "my-kafka-certs" / "my-kafka-certs"


def pem_path(value):
    path = Path(value).expanduser().resolve()
    if not path.is_file():
        raise ValueError(f"Certificate/key file does not exist: {path}")
    return str(path)


def client_config(args):
    config = {
        "bootstrap.servers": args.bootstrap_servers,
        "client.id": "python-mtls-consumer",
        "security.protocol": "SSL",
        "ssl.ca.location": pem_path(args.ca),
        "ssl.certificate.location": pem_path(args.cert),
        "ssl.key.location": pem_path(args.key),
        "enable.ssl.certificate.verification": True,
        "ssl.endpoint.identification.algorithm": "https",
        "group.id": f"python-mtls-reader-{uuid.uuid4().hex}",
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "auto.offset.reset": "earliest",
        "allow.auto.create.topics": False,
        "socket.timeout.ms": int(args.timeout * 1000),
    }
    # Optional passphrase for an encrypted PEM private key; never print it.
    key_password = os.environ.get("KAFKA_SSL_KEY_PASSWORD")
    if key_password:
        config["ssl.key.password"] = key_password
    return config


def consume(config, topic, timeout, max_messages):
    from confluent_kafka import Consumer, KafkaException, OFFSET_BEGINNING, TopicPartition

    consumer = Consumer(config)
    try:
        metadata = consumer.list_topics(timeout=timeout)
        details = metadata.topics.get(topic)
        if details is None:
            raise ValueError(f"Topic {topic!r} does not exist or is not visible to this user.")
        if details.error is not None:
            raise KafkaException(details.error)
        if not details.partitions:
            raise ValueError(f"Topic {topic!r} has no available partitions.")
        # Direct assignment overrides committed offsets for every current partition.
        consumer.assign([
            TopicPartition(topic, partition, OFFSET_BEGINNING)
            for partition in sorted(details.partitions)
        ])
        print(f"Reading {topic!r} from the earliest retained records; Ctrl+C to stop.", file=sys.stderr)
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
    parser.add_argument("--topic", required=True, default="test-topic",help="Topic to read from the beginning.")
    parser.add_argument("--bootstrap-servers", default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS"),
                        help="Comma-separated host:port addresses, or set KAFKA_BOOTSTRAP_SERVERS.")
    parser.add_argument("--ca", default=os.environ.get("KAFKA_SSL_CA_LOCATION", str(CERT_DIR / "ca.crt")),
                        help="Trusted broker CA PEM file.")
    parser.add_argument("--cert", default=os.environ.get("KAFKA_SSL_CERTIFICATE_LOCATION", str(CERT_DIR / "client.crt")),
                        help="Client certificate PEM file.")
    parser.add_argument("--key", default=os.environ.get("KAFKA_SSL_KEY_LOCATION", str(CERT_DIR / "client.key")),
                        help="Client private key PEM file.")
    parser.add_argument("--max-messages", type=int, help="Stop after this many records; otherwise keep listening.")
    parser.add_argument("--timeout", type=float, default=30, help="Metadata/socket timeout in seconds (default: 30).")
    args = parser.parse_args()
    if not args.topic.strip():
        parser.error("--topic must not be empty")
    if not args.bootstrap_servers or not args.bootstrap_servers.strip():
        parser.error("Provide --bootstrap-servers or set KAFKA_BOOTSTRAP_SERVERS")
    if args.max_messages is not None and args.max_messages <= 0:
        parser.error("--max-messages must be greater than zero")
    if not math.isfinite(args.timeout) or not 0.001 <= args.timeout <= 3600:
        parser.error("--timeout must be between 0.001 and 3600 seconds")
    try:
        from confluent_kafka import KafkaException
    except ImportError:
        print("Missing dependency: install confluent-kafka from the supplied requirements file.", file=sys.stderr)
        return 1
    try:
        consume(client_config(args), args.topic, args.timeout, args.max_messages)
        return 0
    except KeyboardInterrupt:
        print("Stopped.", file=sys.stderr)
        return 130
    except (ValueError, OSError, KafkaException) as error:
        print(f"Kafka consumer failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())