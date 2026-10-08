ARG CP_VERSION=7.9.10
FROM confluentinc/cp-kafka-connect:${CP_VERSION}

USER root

ARG S3_CONNECTOR_VERSION

RUN set -eu; \
    : "${S3_CONNECTOR_VERSION:?Supply a pinned S3_CONNECTOR_VERSION build argument}"; \
    confluent-hub install --no-prompt \
        "confluentinc/kafka-connect-s3:${S3_CONNECTOR_VERSION}"

ENV CONNECT_PLUGIN_PATH="/usr/share/java,/usr/share/confluent-hub-components"

USER appuser