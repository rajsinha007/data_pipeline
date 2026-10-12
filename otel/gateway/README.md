# OpenTelemetry Kafka-to-Prometheus Gateway

This OpenTelemetry Collector gateway consumes OTLP-JSON metrics from Kafka and writes them to the Prometheus remote-write API. Deploy this gateway after creating the two Kafka Secrets it references.

## Data flow

`Kafka topic otel-metrics-json` → Kafka receiver → Prometheus remote write

The gateway manifest is [otel-gateway.yml](otel-gateway.yml). It expects Kafka SASL/SCRAM-SHA-512 credentials and a client TLS certificate, and writes to the remote-write endpoint configured in that manifest.

## Prerequisites

- `oc` is logged into the target cluster and can create Secrets and deploy the OpenTelemetryCollector in `its-otel-oss`.
- The OpenTelemetry Operator and the `OpenTelemetryCollector` custom resource are installed.
- Obtain the Kafka username/password, the Kafka client certificate and matching private key, and the CA certificate that verifies the Kafka broker certificates. The gateway config uses SCRAM-SHA-512 plus mutual TLS.
- Kafka brokers and the Prometheus remote-write endpoint are reachable from the namespace. The Prometheus-compatible destination must accept remote writes at the configured URL.
- Do not commit credentials, private keys, or populated Secret manifests to source control.

## 1. Create the SASL credential Secret

Use an environment file outside the repository, for example `kafka-sasl.env`, containing exactly these keys:

```text
username=YOUR_KAFKA_USERNAME
password=YOUR_KAFKA_PASSWORD
```

Create or update the Secret in the same namespace as the gateway. In PowerShell, run from the directory containing the environment file:

```powershell
$namespace = "its-otel-oss"
oc create secret generic kafka-sasl `
  --from-env-file=.\kafka-sasl.env `
  --namespace $namespace `
  --dry-run=client -o yaml | oc apply -f -
```

Restrict access to the file and remove it securely when it is no longer needed. Avoid putting actual credentials directly in the command line, where they may be saved in shell history.

## 2. Create the Kafka mTLS Secret

Have the Kafka administrator provide the PEM-encoded CA certificate, client certificate, and matching client private key. The filenames below can be changed, but the Secret data keys must match the gateway's mounted paths: `ca.crt`, `tls.crt`, and `tls.key`.

```powershell
$namespace = "its-otel-oss"
oc create secret generic kafka-mtls `
  --from-file=ca.crt="C:\path\to\kafka-ca.crt" `
  --from-file=tls.crt="C:\path\to\client.crt" `
  --from-file=tls.key="C:\path\to\client.key" `
  --namespace $namespace `
  --dry-run=client -o yaml | oc apply -f -
```

The CA must trust the certificates served by the Kafka brokers. The client certificate and private key must be a matching pair and the Kafka listener must authorize that client certificate.

## 3. Verify the Secrets without printing values

```powershell
oc get secret kafka-sasl kafka-mtls -n its-otel-oss
oc get secret kafka-sasl -n its-otel-oss -o go-template='{{range $key, $value := .data}}{{$key}}{{"\n"}}{{end}}'
oc get secret kafka-mtls -n its-otel-oss -o go-template='{{range $key, $value := .data}}{{$key}}{{"\n"}}{{end}}'
```

Expected keys:

- `kafka-sasl`: `username`, `password`
- `kafka-mtls`: `ca.crt`, `tls.crt`, `tls.key`

## 4. Deploy the gateway

Run from this directory after both Secrets exist:

```powershell
oc apply -f .\otel-gateway.yml
oc get opentelemetrycollector gateway -n its-otel-oss
oc get pods -n its-otel-oss -o wide
oc logs -n its-otel-oss deployment/gateway-collector --all-containers=true --follow
```

Check the OpenTelemetryCollector status and collector logs for Kafka authentication/TLS errors, missing Secret keys, or remote-write errors. The gateway deployment's pod needs to be restarted after changing a mounted Secret if the operator does not automatically roll it out.

## Notes

- Kafka receiver configuration belongs under `receivers.kafka.metrics`; `topics` and `encoding` are not valid top-level receiver keys.
- `initial_offset: latest` means a new consumer group starts at the latest Kafka offset and does not replay messages already in the topic. The configured consumer group is `otel-gateway-metrics`.
- The Prometheus remote-write receiver must be enabled on the destination. For Prometheus, this generally requires starting it with `--web.enable-remote-write-receiver`; verify this with the Prometheus administrator.
- Secret values are base64-encoded by Kubernetes, not encrypted by base64. Apply cluster access controls and the organization's encryption-at-rest policy to protect them.
