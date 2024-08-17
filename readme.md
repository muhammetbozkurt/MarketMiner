


## Prepare Cluster
### Create cluster with kind

```bash
kind create cluster --name market-miner-cluster
```

### Apply secrets

```bash
kubectl apply -f config-secrets/
```

### Install minio

```bash
helm install minio -f minio/values.yaml oci://registry-1.docker.io/bitnamicharts/minio
```

___Note:___ Check for details [link](https://artifacthub.io/packages/helm/bitnami/minio)

__version:__ 14.7.0


### Install postgresql

```bash
helm install postgresql -f postgrsql/values.yaml oci://registry-1.docker.io/bitnamicharts/postgresql
```

___Note:___ Check for details [link](https://artifacthub.io/packages/helm/bitnami/postgresql)
__version:__ 15.5.22
