


## Prepare Cluster
### Create cluster with kind

```bash
kind create cluster --name market-miner-cluster
```

### Apply secrets

```bash
kubectl apply -f config-secrets/
```

### Install Minio

```bash
helm install minio -f minio/values.yaml oci://registry-1.docker.io/bitnamicharts/minio
```

___Note:___ Check for details [link](https://artifacthub.io/packages/helm/bitnami/minio)

__version:__ 14.7.0


### Install Postgresql

```bash
helm install postgresql -f postgrsql/values.yaml oci://registry-1.docker.io/bitnamicharts/postgresql
```

___Note:___ Check for details [link](https://artifacthub.io/packages/helm/bitnami/postgresql)
__version:__ 15.5.22


### Install Spark

```bash
helm install spark oci://registry-1.docker.io/bitnamicharts/spark
```

___Note:___ Check for details [link](https://artifacthub.io/packages/helm/bitnami/postgresql)
__version:__ 9.2.9

