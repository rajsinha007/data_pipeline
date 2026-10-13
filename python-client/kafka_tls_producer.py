#!/usr/bin/env python3
"""Publish UTF-8 records to Kafka using mTLS (no SASL)."""

import argparse
import math
import os
import sys
import time
from contextlib import nullcontext
from pathlib import Path

from kafka_tls_consumer import CERT_DIR, pem_path


def client_config(args):
    config = {
        "bootstrap.servers": args.bootstrap_servers,
        "client.id": "python-mtls-producer",
        "security.protocol": "SSL",
        "ssl.ca.location": pem_path(args.ca),
        "ssl.certificate.location": pem_path(args.cert),
        "ssl.key.location": pem_path(args.key),
        "enable.ssl.certificate.verification": True,
        "ssl.endpoint.identification.algorithm": "https",
        "enable.idempotence": True,
        "acks": "all",
        "delivery.timeout.ms": int(args.timeout * 1000),
        "socket.timeout.ms": int(args.timeout * 1000),
    }
    password = os.environ.get("KAFKA_SSL_KEY_PASSWORD")
    if password:
        config["ssl.key.password"] = password
    return config


def records(args):
    if args.message is not None:
        yield args.message
        return
    source = Path(args.file).open(encoding="utf-8") if args.file else nullcontext(sys.stdin)
    if not args.file and sys.stdin.isatty():
        print("Enter one record per line; end input with Ctrl+Z then Enter on Windows.", file=sys.stderr)
    with source as stream:
        for line in stream:
            yield line.rstrip("\r\n")


def publish(args):
    from confluent_kafka import KafkaException, Producer

    producer = Producer(client_config(args))
    errors = []
    delivered = 0
    queued = 0

    def on_delivery(error, message):
        nonlocal delivered
        if error is not None:
            errors.append(str(error))
        else:
            delivered += 1
            print(f"Delivered: {message.topic()} [{message.partition()}] offset {message.offset()}", flush=True)

    try:
        metadata = producer.list_topics(timeout=args.timeout)
        topic = metadata.topics.get(args.topic)
        if topic is None:
            raise ValueError(f"Topic {args.topic!r} does not exist or is not visible; create it first.")
        if topic.error is not None:
            raise KafkaException(topic.error)
        for text in records(args):
            deadline = time.monotonic() + args.timeout
            while True:
                if errors:
                    raise RuntimeError("Delivery failed: " + errors[0])
                try:
                    producer.produce(args.topic, value=text.encode("utf-8"),
                                     key=args.record_key.encode("utf-8") if args.record_key is not None else None,
                                     on_delivery=on_delivery)
                    queued += 1
                    break
                except BufferError:
                    if time.monotonic() >= deadline:
                        raise RuntimeError("Producer queue remained full; input was not fully sent.")
                    producer.poll(min(0.1, max(0, deadline - time.monotonic())))
            producer.poll(0)
    finally:
        remaining = producer.flush(args.timeout)
        print(f"Queued: {queued}; acknowledged: {delivered}; failed: {len(errors)}; pending: {remaining}",
              file=sys.stderr)
    if errors:
        raise RuntimeError("Delivery failed: " + errors[0])
    if remaining:
        raise RuntimeError("Delivery timed out; pending records have unconfirmed delivery.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--bootstrap-servers", default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS"))
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--message", help="Send one UTF-8 record.")
    source.add_argument("--file", help="Send each line of a UTF-8 file as a record; otherwise read stdin.")
    parser.add_argument("--record-key", help="Optional partitioning key for all records.")
    parser.add_argument("--ca", default=os.environ.get("KAFKA_SSL_CA_LOCATION", str(CERT_DIR / "ca.crt")))
    parser.add_argument("--cert", default=os.environ.get("KAFKA_SSL_CERTIFICATE_LOCATION", str(CERT_DIR / "client.crt")))
    parser.add_argument("--key", default=os.environ.get("KAFKA_SSL_KEY_LOCATION", str(CERT_DIR / "client.key")))
    parser.add_argument("--timeout", type=float, default=30, help="Metadata/delivery timeout in seconds.")
    args = parser.parse_args()
    if not args.topic.strip():
        parser.error("--topic must not be empty")
    if not args.bootstrap_servers or not args.bootstrap_servers.strip():
        parser.error("Provide --bootstrap-servers or set KAFKA_BOOTSTRAP_SERVERS")
    if not math.isfinite(args.timeout) or not 1 <= args.timeout <= 3600:
        parser.error("--timeout must be between 1 and 3600 seconds")
    try:
        from confluent_kafka import KafkaException
    except ImportError:
        print("Missing dependency: install confluent-kafka from the supplied requirements file.", file=sys.stderr)
        return 1
    try:
        publish(args)
        return 0
    except KeyboardInterrupt:
        print("Interrupted; review delivery acknowledgements before retrying.", file=sys.stderr)
        return 130
    except (ValueError, OSError, KafkaException, RuntimeError) as error:
        print(f"Kafka producer failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())