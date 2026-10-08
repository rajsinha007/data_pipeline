# Kafka Connect S3 Sink Deployment

The manifest deploys a three-replica Kafka Connect cluster and exposes its REST API through a `ClusterIP` Service on port 8083. An init container downloads the S3 connector plugin. The sink definition in `s3-sink-connector.json` writes JSON records from Kafka topic `test1` to the configured S3-compatible bucket and endpoint.

## Prerequisites

- Deploy into a namespace that can reach Kafka and the S3-compatible endpoint.
- Create `kafka-credentials` with key `sasl.jaas.config` and `s3-credentials` with keys `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` in that namespace.
- Ensure the plugin download URL configured in `manifest.yaml` is valid. Avoid committing presigned URLs or credentials; keep them in Kubernetes Secrets.

## Deploy

Run from this directory in PowerShell, replacing the namespace:

```powershell
$namespace = "YOUR_NAMESPACE"
kubectl apply -n $namespace -f .\manifest.yaml
kubectl rollout status -n $namespace deployment/kafka-connect-s3
kubectl get pods,svc -n $namespace -l app=kafka-connect-s3
```

## Check the API and create the sink

In one terminal, forward the REST service:

```powershell
kubectl port-forward -n YOUR_NAMESPACE svc/kafka-connect-s3 8083:8083
```

In another PowerShell terminal, verify the S3 plugin is available:

```powershell
Invoke-RestMethod -Uri "http://localhost:8083/connector-plugins" |
	Where-Object { $_.class -eq "io.confluent.connect.s3.S3SinkConnector" }
```

Create the sink using the JSON request body in the file (it includes the connector name and configuration):

```powershell
Invoke-RestMethod -Method Post `
	-Uri "http://localhost:8083/connectors" `
	-ContentType "application/json" `
	-InFile ".\s3-sink-connector.json"
```

Check connector and task state:

```powershell
Invoke-RestMethod -Uri "http://localhost:8083/connectors/s3-sink/status" |
	ConvertTo-Json -Depth 10
```

The connector batches up to 1,000 records and also rotates on its configured schedule. Check the connector status and S3 bucket if records are not appearing. To update an existing connector, use `PUT /connectors/s3-sink/config` with the `config` object only; `POST /connectors` creates a new connector.