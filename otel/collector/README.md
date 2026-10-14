# Deploy the OpenTelemetry node agent to production clusters

This guide is for the platform team deploying the node-level OpenTelemetry Collector agent to each production OpenShift cluster. The agent runs as a DaemonSet, reads host metrics, scrapes the local Cilium Hubble endpoint, and publishes metrics to Kafka over mutual TLS.

Manifest: [collector.yml](collector.yml)

> **Production configuration required:** `collector.yml` is currently configured for the Czech test cluster (`K8S_CLUSTER_NAME=czchooct1237`, `DC_REGION=cz`, `DC_NAME=Chodov`, `DC_ENVIRONMENT=test`), Czech Kafka broker endpoints, and topic `otel.metrics.czchooct1237`. Set `DC_ENVIRONMENT` to exactly `test` or `production` for the target cluster. Do not apply this manifest unchanged to another cluster. Make a reviewed per-cluster copy or update these values for the target cluster. Do not commit production credentials or private keys.

## Per-cluster prerequisites

- `oc` is logged in with permissions to create resources in `its-otel-oss`, bind SCCs, and deploy the OpenTelemetryCollector resource. Confirm the active cluster before proceeding.
- The OpenTelemetry Operator and `opentelemetry.io/v1beta1` CRD are installed in the target cluster.
- Cilium is installed and Hubble metrics are enabled on every node. The current agent manifest scrapes `localhost:9965`; ensure the Hubble metrics endpoint is listening and reachable at that address from the host-networked agent pods, or update the scrape target and port for the cluster's Hubble configuration. Select and enable the required Hubble metric sources in Cilium; otherwise the scrape can succeed but return no metrics.
- The Kafka brokers are reachable from the cluster, and the target topic exists. The client certificate must be trusted by Kafka and authorized to write to the configured topic.
- Obtain the Kafka CA certificate, client certificate, and private key through the organization's approved PKI/secret delivery process. The resulting Secret must contain exactly the keys `ca.crt`, `tls.crt`, and `tls.key`.
- Confirm namespace quota allows the agent's CPU and memory requests and limits. The current manifest requests `50m` CPU and `128Mi` memory, with limits of `500m` CPU and `256Mi` memory. Review these values against cluster workload and quota; increase them if the collector is CPU-throttled or memory pressure is observed.
- The DaemonSet is configured to tolerate the standard `node-role.kubernetes.io/control-plane:NoSchedule` and legacy `node-role.kubernetes.io/master:NoSchedule` taints, so it can run on control-plane/master nodes as well as workers. Confirm platform policy permits workloads on control-plane nodes and that node taints/affinity in the target cluster do not impose additional scheduling constraints.
- Review and approve the host access security exception below. This agent uses host networking and a read-only mount of the host root filesystem.

## Deploy on each production cluster

Run these steps separately for every target cluster. Commands are provided for both Linux Bash and Windows PowerShell where shell syntax differs; the simple `oc` commands work in either shell. Substitute the target cluster's metadata, Kafka endpoints, topic, and certificate files in the reviewed manifest and secret inputs. Ensure `oc` is installed and authenticated to the intended cluster.

### 1. Confirm cluster and prepare namespace/service account

```bash
oc whoami --show-server
oc project its-otel-oss
```

If the namespace does not exist, have an authorized platform operator create it. Create the service account before granting the SCC:

```bash
oc create serviceaccount otel-agent -n its-otel-oss --dry-run=client -o yaml | oc apply -f -
```

### 2. Grant the required SCC to the agent service account

Run as an account authorized to administer SCCs, in each target cluster:

```bash
oc adm policy add-scc-to-user hostaccess -z otel-agent -n its-otel-oss
```

This is a high-privilege host-access grant. Keep it scoped to this service account and namespace; do not grant it to the default service account or broadly to users. If platform security does not approve `hostaccess`, stop and have the security/platform team provide an approved least-privilege SCC and adjust the workload accordingly.

### 3. Create/update the Kafka mTLS Secret

From a secure workstation holding the approved files, create or update the Secret without writing its contents into a manifest or displaying them. Use the appropriate shell option and replace the certificate directory with the approved secure location.

**Linux (Bash):**

```bash
set -euo pipefail
cert_dir="/secure/path/to/cluster-kafka-certs"
oc create secret generic kafka-mtls -n its-otel-oss \
  --from-file=ca.crt="${cert_dir}/ca.crt" \
  --from-file=tls.crt="${cert_dir}/client.crt" \
  --from-file=tls.key="${cert_dir}/client.key" \
  --dry-run=client -o yaml | oc apply -f -
```

**Windows (PowerShell):**

```powershell
$ErrorActionPreference = 'Stop'
$certDir = 'C:\secure\path\to\cluster-kafka-certs'
oc create secret generic kafka-mtls -n its-otel-oss `
  --from-file="ca.crt=$certDir/ca.crt" `
  --from-file="tls.crt=$certDir/client.crt" `
  --from-file="tls.key=$certDir/client.key" `
  --dry-run=client -o yaml | oc apply -f -
if ($LASTEXITCODE -ne 0) { throw 'Secret creation or apply failed' }
```

Verify only the Secret metadata and key names; never print Secret data:

```bash
oc describe secret kafka-mtls -n its-otel-oss
```

Expected keys: `ca.crt`, `tls.crt`, `tls.key`. The Secret is namespace-scoped and must be created separately in every target cluster.

### 4. Apply the per-cluster agent manifest

After replacing all Czech/test-specific values and reviewing the resulting configuration, apply the manifest:

```bash
oc apply -f ./collector.yml
```

The manifest includes the `otel-agent` ServiceAccount, so applying it is safe after the SCC grant and Secret are in place. Verify the OpenTelemetryCollector and DaemonSet:

```bash
oc get opentelemetrycollector agent -n its-otel-oss
oc get daemonsets,pods -n its-otel-oss -o wide
```

Wait until the agent DaemonSet has the desired number of pods ready across worker and control-plane/master nodes. Verify pod placement with `oc get pods -o wide`; inspect collector logs for TLS, Kafka authorization, exporter, receiver, or scrape errors:

```bash
oc get pods -n its-otel-oss -o wide
oc logs -n its-otel-oss <agent-pod-name> -c otc-container --tail=100
```

Finally, verify that metrics arrive in the configured Kafka topic and that downstream consumers can read them. Check cluster identity/resource attributes (`k8s.cluster.name`, `dc.region`, `dc.name`, `dc.environment`, and `host.name`) to ensure the production cluster is labeled correctly.

## Operations and rollback

- After rotating the Kafka client certificate, update `kafka-mtls` and restart the agent DaemonSet so pods load the new mounted certificate: `oc rollout restart daemonset/<agent-daemonset-name> -n its-otel-oss`.
- For a planned removal, delete the OpenTelemetryCollector resource first and confirm the DaemonSet/pods are gone. Remove the SCC binding only after no workload uses the service account:

```bash
oc delete opentelemetrycollector agent -n its-otel-oss
oc adm policy remove-scc-from-user hostaccess -z otel-agent -n its-otel-oss
```

- Coordinate topic ACL, certificate rotation, cluster-specific configuration, and rollout windows with the Kafka and platform teams. Do not copy private keys into Git, tickets, or chat.
