# Kafka Helm Deployment

Deploy the local Kafka Helm chart using the site-specific settings in `kafka/custom-config.yaml`.

## Custom configuration

- Runs three Kafka controller-eligible nodes with vSphere-backed 2 GiB persistent volumes.
- Sets replication and minimum in-sync replicas to three; logs are retained for seven days.
- Enables an external SASL/TLS listener with required client certificates and generated certificates for the configured DNS names, served through a `ClusterIP` on port 443.
- SASL credentials are currently in the values file; use a Kubernetes Secret for production.

From this directory, run:

```sh
helm upgrade --install kafka ./kafka \
  --namespace kafka --create-namespace \
  -f ./kafka/custom-config.yaml
```

Verify the deployment with `kubectl get pods,svc -n kafka` and `helm status kafka -n kafka`. The external listener requires the configured DNS names, client certificates, and SASL credentials. Store credentials in a Kubernetes Secret rather than in the values file for production use.
