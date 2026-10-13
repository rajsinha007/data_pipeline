# OpenTelemetry Kafka-to-Prometheus Gateway

This OpenTelemetry Collector gateway consumes OTLP-JSON metrics from the Czech Kafka cluster using its internal SCRAM listener and writes them to the Prometheus remote-write API. Deploy it after provisioning the gateway user and Secret.

## Data flow

`Kafka topic otel.metrics.czchooct1237` → Kafka receiver → Prometheus remote write

The gateway manifest is [otel-gateway.yml](otel-gateway.yml). It connects to `kafka-kafka-bootstrap.its-dppc-kafka-prg.svc.cluster.local:9092` using `SCRAM-SHA-512` over `SASL_PLAINTEXT`. This connection is authenticated but not encrypted and requires same-cluster network access to Kafka and its advertised brokers.

## Prerequisites

- `oc` is logged into the target cluster and can create Secrets and deploy the OpenTelemetryCollector in `its-otel-oss`.
- The OpenTelemetry Operator and the `OpenTelemetryCollector` custom resource are installed.
- Create the `otel.metrics.czchooct1237` topic. The dedicated [gateway KafkaUser](../../kafka/czchoocp1036.dhl.com/kafka/otel-gateway-user.yaml) grants Read/Describe on this topic and consumer group `otel-gateway-metrics`.
- The Strimzi User Operator must be available in `its-dppc-kafka-prg`; permission to create KafkaUsers and read its generated Secret is required.
- Kafka brokers and the Prometheus remote-write endpoint are reachable from the namespace. The Prometheus-compatible destination must accept remote writes at the configured URL.
- Do not commit credentials, private keys, or populated Secret manifests to source control.

## 1. Create the Kafka user and gateway Secret

Run from this directory. Strimzi generates the password; do not commit it. Secrets cannot be referenced across namespaces, so copy only the password into `its-otel-oss` without printing it or writing it to disk.

```powershell
$ErrorActionPreference = 'Stop'
oc apply -n its-dppc-kafka-prg -f ..\..\kafka\czchoocp1036.dhl.com\kafka\otel-gateway-user.yaml
if ($LASTEXITCODE -ne 0) { throw 'KafkaUser apply failed' }
oc wait -n its-dppc-kafka-prg --for=condition=Ready kafkauser/otel-gateway --timeout=120s
if ($LASTEXITCODE -ne 0) { throw 'KafkaUser is not ready' }
$source = oc get secret otel-gateway -n its-dppc-kafka-prg -o json | ConvertFrom-Json
if ($LASTEXITCODE -ne 0 -or -not $source.data.password) { throw 'Generated password is unavailable' }
@{
  apiVersion = 'v1'
  kind = 'Secret'
  metadata = @{ name = 'otel-gateway'; namespace = 'its-otel-oss' }
  type = 'Opaque'
  data = @{ password = $source.data.password }
} | ConvertTo-Json -Depth 5 | oc apply -f -
if ($LASTEXITCODE -ne 0) { throw 'Gateway Secret apply failed' }
Remove-Variable source
```

Repeat the Secret copy after password rotation and restart the gateway so its environment uses the new password. Do not remove `kafka-mtls` if the agent still uses it.

## 2. Verify the Secret without printing values

```powershell
oc describe secret otel-gateway -n its-otel-oss
```

Expected key: `password`. The command displays key sizes, not values.

## 3. Deploy the gateway

Run from this directory after the Secret exists:

```powershell
oc apply -f .\otel-gateway.yml
oc get opentelemetrycollector gateway -n its-otel-oss
oc get pods -n its-otel-oss -o wide
oc logs -n its-otel-oss deployment/gateway-collector --all-containers=true --follow
```

Check the OpenTelemetryCollector status and collector logs for Kafka authentication/authorization errors, missing Secret keys, or remote-write errors. After updating the password Secret, run `oc rollout restart deployment/gateway-collector -n its-otel-oss`.

## Notes

- Kafka receiver configuration belongs under `receivers.kafka.metrics`; `topics` and `encoding` are not valid top-level receiver keys.
- `initial_offset: latest` means a new consumer group starts at the latest Kafka offset and does not replay messages already in the topic. The configured consumer group is `otel-gateway-metrics`.
- The Prometheus remote-write receiver must be enabled on the destination. For Prometheus, this generally requires starting it with `--web.enable-remote-write-receiver`; verify this with the Prometheus administrator.
- Secret values are base64-encoded by Kubernetes, not encrypted by base64. Apply cluster access controls and the organization's encryption-at-rest policy to protect them.
