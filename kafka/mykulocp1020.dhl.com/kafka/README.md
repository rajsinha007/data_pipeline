# Strimzi Kafka on OpenShift

This directory deploys cluster `kafka` using the Strimzi/AMQ Streams Operator,
not the legacy Bitnami Helm chart. It does not migrate data from that deployment.

## Manifests

| File | Resources |
| --- | --- |
| [kafka.yaml](kafka.yaml) | Kafka `kafka` and three-node KafkaNodePool `broker-controller` |
| [admin-user.yaml](admin-user.yaml) | TLS KafkaUser `admin` and topic/group ACLs |
| [test-topic.yaml](test-topic.yaml) | KafkaTopic `test-topic`: three partitions, three replicas, minimum ISR two, seven-day retention |
| [otel-metrics-topic.yaml](otel-metrics-topic.yaml) | KafkaTopic `otel.metrics.czchooct1237`: three partitions, three replicas, minimum ISR two, seven-day retention |

All manifests omit `metadata.namespace`. The commands below explicitly use
`its-dppc-kafka-kul`; use the same namespace for all resources and configure the
operator to watch it. The cluster manifest contains no KafkaUser or KafkaTopic.

## Prerequisites

- Install a Strimzi Cluster Operator supporting KRaft and the `kafka.strimzi.io/v1`
	API for KafkaNodePool, Kafka, KafkaUser, and KafkaTopic. Configure it to watch
	`its-dppc-kafka-kul`. Check installed operator/API compatibility before applying.
- Ensure namespace `its-dppc-kafka-kul` exists and sufficient CPU, memory, and
	storage quota is available, including rolling-update headroom.
- Ensure the `csi-vsphere-block` storage class exists, or change `spec.storage.class` in the node pool.
- Point `kafka-my.dhl.com` and `kafka-{0,1,2}-my.dhl.com` at the correct OpenShift
	ingress endpoint. Allow client access to all four hosts on TCP 443. Creating
	Routes does not create DNS records; do not reuse hosts claimed by another cluster.
- Ensure the ingress router accepts TLS passthrough routes with these custom hosts. Strimzi creates the Routes and certificates; do not create edge-terminated Routes for this listener.

## Configuration

- Three combined KRaft controller/broker pods, each with a 4 GiB PVC (12 GiB provisioned total). PVCs are retained when the cluster is deleted.
- Node pool `broker-controller`: requests 200m CPU / 512 MiB per node; limits
	500m CPU / 1 GiB. Topic/User Operators each request 100m CPU / 128 MiB and limit
	200m CPU / 256 MiB. These are test-sized resources, not production capacity recommendations.
- Kafka version is not pinned; the installed operator selects its default version.
- Replication factor three for new automatically created topics and Kafka offset/transaction topics.
- Minimum ISR two, allowing writes with one broker unavailable when producers use `acks=all`. Change both minimum ISR settings to three if writes must require all three replicas; writes will then stop whenever one replica is unavailable.
- Default time-based retention seven days (`604800000` milliseconds), with no configured size-based retention limit. Topic-level overrides take precedence; existing topics are not automatically assigned replication factor three.
- External **mTLS** (`authentication.type: tls`) through TLS passthrough Routes.
	Client protocol is `SSL`, without SASL. The Kafka listener is 9094 internally;
	clients connect to `kafka-my.dhl.com:443` and advertised per-broker hosts on 443.
- Internal `SASL_PLAINTEXT` using SCRAM-SHA-512 on port 9092, through
	`kafka-kafka-bootstrap.its-dppc-kafka-kul.svc:9092`. Traffic is unencrypted;
	restrict it with network policies or configure TLS for production requirements.
- Authorization is ACL-based (`type: simple`). The separate `admin` TLS user
	allows Read/Write/Describe/Create on all topics and groups, exactly as declared
	in its manifest. Its name does not make it a superuser or grant cluster-level
	administration. Some group operations are not needed for normal consumers;
	prefer only Read/Describe. Create dedicated application users with scoped ACLs.
- No SCRAM user is included, so internal access requires a separately provisioned
	SCRAM KafkaUser. The TLS `admin` credentials cannot authenticate on that listener.

## Deployment

From this directory, after completing the prerequisites:

```powershell
oc apply -n its-dppc-kafka-kul -f kafka.yaml
oc wait kafka/kafka -n its-dppc-kafka-kul --for=condition=Ready --timeout=600s
oc apply -n its-dppc-kafka-kul -f admin-user.yaml
oc wait kafkauser/admin -n its-dppc-kafka-kul --for=condition=Ready --timeout=300s
oc apply -n its-dppc-kafka-kul -f test-topic.yaml
oc wait kafkatopic/test-topic -n its-dppc-kafka-kul --for=condition=Ready --timeout=300s
oc apply -n its-dppc-kafka-kul -f otel-metrics-topic.yaml
oc wait kafkatopic/otel.metrics.czchooct1237 -n its-dppc-kafka-kul --for=condition=Ready --timeout=300s
oc get kafkanodepool,kafka,kafkauser,kafkatopic,pods,pvc,route -n its-dppc-kafka-kul
```

Inspect the Kafka status if readiness fails. Do not deploy this alongside an existing cluster unless the additional storage/resources and route hosts are available. This does not migrate data from the Helm deployment.

Initial pod names are `kafka-broker-controller-0`, `kafka-broker-controller-1`,
and `kafka-broker-controller-2`. The manifest's broker hosts assume IDs 0–2;
verify actual node IDs before modifying the node pool. Renaming a cluster/pool
creates different identities, not an in-place rename. New topics initially have no records.

## Client configuration

```properties
bootstrap.servers=kafka-my.dhl.com:443
security.protocol=SSL
ssl.truststore.type=PEM
ssl.truststore.location=/secure/cluster-ca.crt
ssl.keystore.type=PKCS12
ssl.keystore.location=/secure/user.p12
ssl.keystore.password=<password-from-TLS-user-secret>
ssl.endpoint.identification.algorithm=https
# Producer settings:
acks=all
enable.idempotence=true
```

Use decoded `ca.crt` from Secret **`kafka-cluster-ca-cert`** to verify brokers.
Use `user.p12` and `user.password` from Secret **`admin`**, or your dedicated TLS
application user's Secret. PEM clients instead use `user.crt` and `user.key`.
Kubernetes Secret data is base64-encoded.

The user Secret's `ca.crt` is the **clients CA**, not the cluster CA: it does not
provide the correct trust anchor for broker verification. Keep hostname and
certificate verification enabled. Never distribute broker private keys as client
credentials. Protect client keys/passwords with approved secret management,
not email, Git, or shared/synchronized folders.

### Kubernetes internal clients

For applications running inside Kubernetes, use:

```properties
bootstrap.servers=kafka-kafka-bootstrap.its-dppc-kafka-kul.svc:9092
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="<SCRAM-user>" password="<password-from-SCRAM-user-secret>";
acks=all
```

Provision a separate SCRAM-SHA-512 KafkaUser first. No TLS trust store is required
on this listener. `kafka-kafka-bootstrap:9092` works in the same namespace.
Network policies must allow bootstrap and every broker on 9092 while restricting
access to trusted applications because message traffic is unencrypted.

### Clients inside a Kafka broker pod

Use `bootstrap.servers=localhost:9092` with `security.protocol=SASL_PLAINTEXT` and the same SCRAM-SHA-512 credentials as the internal client configuration above. No TLS certificate or trust store is required. Kafka metadata still returns internal broker DNS names, which must be reachable from the pod. This works only inside a broker pod (including a sidecar sharing its network namespace), not an unrelated application pod. The listener is not restricted to loopback; authentication and ACLs still apply.

### Local workstation clients

Workstation clients use `kafka-my.dhl.com:443` with TLS client certificates.
All advertised broker hostnames must be reachable and match certificate SANs.
Port 443 is Kafka-over-TLS, not HTTPS; proxies must preserve TLS/SNI passthrough.

A single bootstrap port-forward does not expose every advertised broker to a
workstation. Literal localhost access requires a tunneling/listener design covering
all brokers; do not replace internal advertised addresses with localhost.

### Python producer and consumer

From the shared [Python client directory](../../../python-client/):

```powershell
python -m pip install -r requirements.txt
python kafka_tls_producer.py --bootstrap-servers kafka-my.dhl.com:443 --topic test-topic --message "Hello from mTLS"
python kafka_tls_consumer.py --bootstrap-servers kafka-my.dhl.com:443 --topic test-topic
```

Use `--ca`, `--cert`, and `--key` to select the current cluster CA and TLS user
credentials. Defaults reference the Malaysia certificate directory relative to
the scripts. Environment overrides are `KAFKA_SSL_CA_LOCATION`,
`KAFKA_SSL_CERTIFICATE_LOCATION`, and `KAFKA_SSL_KEY_LOCATION`; encrypted keys
can use `KAFKA_SSL_KEY_PASSWORD` supplied securely.

The producer accepts a message, a UTF-8 line file (`--file`), or stdin, and prints
delivery acknowledgements. The consumer explicitly starts at the earliest retained
offset of each partition present at startup and keeps listening; `--max-messages`
can limit how many records it reads. It does not commit offsets; use it for
diagnostics/replay, not unchanged production group processing.

## Certificate renewal

Strimzi renews managed TLS client certificates and updates KafkaUser Secrets.
Default validity is 365 days with a 30-day renewal window unless overridden.
Monitor actual expiry and changes to the public certificate. External clients need
secure distribution of renewed certificate/key pairs and a reload/restart before
expiry. Normal same-CA renewal does not immediately invalidate the old certificate.
CA rotation requires separately tested trust-bundle updates; recreating a cluster
can replace its CAs and invalidate previously distributed credentials.

## Troubleshooting

```powershell
oc describe kafka kafka -n its-dppc-kafka-kul
oc describe strimzipodset kafka-broker-controller -n its-dppc-kafka-kul
oc get events -n its-dppc-kafka-kul --sort-by=.metadata.creationTimestamp
oc get routes -n its-dppc-kafka-kul
```

Check quota/admission errors and operator reconciliation for missing pods; the
cluster CA, expiry, DNS and Route target for TLS errors; and topic readiness/ACLs
for missing topics. A bound PVC alone does not guarantee pod creation.

## Storage caveat

Seven days is a retention policy, not a storage-capacity guarantee. With replication factor three, 12 GiB of physical storage provides less than 4 GiB of usable unique message data after overhead, internal topics, and the KRaft log. Size ingestion accordingly or increase PVC capacity: Kafka does not automatically delete data early just because the disk is full. Retention cleanup is asynchronous and segment-based, so data may remain longer than seven days.

`deleteClaim: false` does not protect against manual PVC deletion. Never use
`oc delete pvc --all` for production troubleshooting. Monitor disk usage, replication,
consumer lag, resource usage, delivery errors, and certificate expiry. Plan recovery,
failure-domain placement, and a pinned supported Kafka version for controlled upgrades.