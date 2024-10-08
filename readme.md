


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
helm upgrade --install minio -f minio/values.yaml oci://registry-1.docker.io/bitnamicharts/minio --version 14.7.0
```

___Note:___ Check for details [link](https://artifacthub.io/packages/helm/bitnami/minio)


### Install Postgresql

```bash
helm upgrade --install postgresql -f postgrsql/values.yaml oci://registry-1.docker.io/bitnamicharts/postgresql --version 15.5.22
```

___Note:___ Check for details [link](https://artifacthub.io/packages/helm/bitnami/postgresql)


### Install Spark

```bash
helm upgrade --install spark oci://registry-1.docker.io/bitnamicharts/spark --version 9.2.9
```

___Note:___ Check for details [link](https://artifacthub.io/packages/helm/bitnami/spark)


### Install Airflow
```bash
kubectl create secret generic my-webserver-secret --from-literal="webserver-secret-key=$(python3 -c 'import secrets; print(secrets.token_hex(16))')" #
helm repo add apache-airflow https://airflow.apache.org
helm repo update
helm upgrade --install airflow apache-airflow/airflow -f airflow/values.yaml --version 1.15.0
```

___Note:___ Check for details [link](https://artifacthub.io/packages/helm/apache-airflow/airflow)


### Prepare Image for Dags
```bash
docker build -t custom-image:1.0.0 
kind load docker-image custom-image:1.0.0 --name market-miner-cluster
```

```bash
docker build -t spark-job:1.0.0 src/spark-job/

kind load docker-image spark-job:1.0.0 --name market-miner-cluster
```