# Python Kafka client: mutual TLS + SASL

Uses `confluent-kafka` with `SASL_SSL`, a PEM client certificate/private key,
broker CA verification and hostname verification. Default SASL mechanism:
`SCRAM-SHA-512`. No credentials are embedded in the script.

## Run in PowerShell

From this directory:

```powershell
python -m pip install -r requirements.txt

# Example Malaysia endpoints; substitute your actual reachable listener addresses.
$env:KAFKA_BOOTSTRAP_SERVERS = "kafka-0-my.dhl.com:443,kafka-1-my.dhl.com:443,kafka-2-my.dhl.com:443"
$env:KAFKA_SASL_USERNAME = "user1"
$env:KAFKA_SASL_MECHANISM = "SCRAM-SHA-512"
$env:KAFKA_SSL_CA_LOCATION = "C:\certs\ca.crt"
$env:KAFKA_SSL_CERTIFICATE_LOCATION = "C:\certs\client.crt"
$env:KAFKA_SSL_KEY_LOCATION = "C:\certs\client.key"

# Prompts for the SASL password without echoing it.
# Reads topic2 from the beginning and continues listening until Ctrl+C.
python kafka_client.py

# Optional: read a different topic, stopping after 10 messages.
python kafka_client.py --topic topic2 --max-messages 10

# Optional: check connectivity only.
python kafka_client.py --check

# Optional: publish and wait for the broker delivery acknowledgement.
python kafka_client.py --topic test-topic --message "Hello from Python"
```

For unattended runs, supply `KAFKA_SASL_PASSWORD` through your secret manager or
runtime environment. For an encrypted PEM private key, also supply
`KAFKA_SSL_KEY_PASSWORD` securely. Never commit passwords or private keys.
`--timeout` controls the metadata and delivery wait separately (default 30 seconds).
Consumption keeps listening even if the topic is currently empty; `--max-messages`
is a record limit, not a time limit. Each run explicitly starts at the earliest
retained offset of every partition and does not commit offsets. Expired or
compacted-away records cannot be recovered. Ordering is guaranteed within each
partition, not across partitions. Values are printed as UTF-8 with replacement
for invalid bytes; tombstones are shown as `<tombstone>`.

## Broker and network requirements

- The listener must use `SASL_SSL` and `ssl.client.auth=required`. In this Helm
  chart the external listener setting is `listeners.external.sslClientAuth: "required"`.
  The Czech workspace configuration currently sets it to `"none"`, which does
  **not** enforce mutual TLS; the active Malaysia configuration sets `"required"`.
- The broker must trust the CA that signed the **client** certificate. The
  client certificate must be valid for client authentication. Use a dedicated
  client certificate, not the broker's certificate/private key.
- `KAFKA_SSL_CA_LOCATION` must contain the CA chain needed to verify the broker.
  The client certificate PEM should include any required intermediate chain.
- SASL credentials must exist on the broker; the selected mechanism must be enabled.
  Supported choices are `PLAIN`, `SCRAM-SHA-256`, and `SCRAM-SHA-512`.
- Every address returned in Kafka's `advertised.listeners` must be reachable
  from this computer and match the broker certificate's subject alternative names.
- Port 443 here is raw Kafka-over-TLS, **not HTTPS**. Any OpenShift route/proxy
  must use TLS passthrough so the Kafka broker receives the client certificate.
- A successful metadata request confirms connectivity and SASL authentication;
  enforcing client-certificate authentication depends on the broker configuration.
  Consuming requires topic read permissions. Manual partition assignment reads all
  partitions present at startup without joining a consumer group; restart the
  script to include partitions added later.
  Publishing additionally requires topic write permissions and an existing topic
  (unless broker auto-creation is intentionally enabled).

TLS verification is never disabled by this script.