README.md


# ETL Pipeline for 4TB Data Processing

This project implements a scalable ETL pipeline for processing 4TB of data from PostgreSQL to BigQuery using Kubernetes.

## Architecture

The pipeline consists of three main components:

1. **Controller**: Orchestrates the extraction and loading jobs
2. **Extractor**: Extracts data from PostgreSQL tables in partitions
3. **Loader**: Loads Parquet files into BigQuery

## Deployment Instructions

### 1. Build Docker Images

```bash
# Build controller image
docker build -t your-registry/etl-controller:latest -f docker/Dockerfile.controller .

# Build extractor image
docker build -t your-registry/data-extractor:latest -f docker/Dockerfile.extractor .

# Build loader image
docker build -t your-registry/data-loader:latest -f docker/Dockerfile.loader .

# Push images to your registry
docker push your-registry/etl-controller:latest
docker push your-registry/data-extractor:latest
docker push your-registry/data-loader:latest
```

### 2. Create Kubernetes Resources

```bash
# Create namespace
kubectl create namespace etl-pipeline

# Create PVC
kubectl apply -f kubernetes/etl-pvc.yaml

# Create ConfigMap with job templates
kubectl apply -f kubernetes/etl-configmap.yaml

# Create secrets for database and BigQuery credentials
kubectl create secret generic db-credentials \
  --from-literal=connection-string='postgresql://user:password@postgres-host:5432/db' \
  -n etl-pipeline

kubectl create secret generic gcp-credentials \
  --from-file=service-account-json=/path/to/service-account.json \
  -n etl-pipeline

# Deploy controller CronJob
kubectl apply -f kubernetes/controller-cronjob.yaml
```

### 3. Monitoring

Monitor the pipeline using:

```bash
# Check CronJob status
kubectl get cronjobs -n etl-pipeline

# Check running jobs
kubectl get jobs -n etl-pipeline

# Check pods
kubectl get pods -n etl-pipeline

# Check logs
kubectl logs -n etl-pipeline -l job-name=<job-name>
```

## Configuration

Adjust the following parameters based on your environment:

1. **Storage**: Update the PVC size and storage class in `etl-pvc.yaml`
2. **Concurrency**: Adjust `MAX_CONCURRENT_EXTRACTIONS` and `MAX_CONCURRENT_LOADS` in the controller CronJob
3. **Schedule**: Change the cron schedule in `controller-cronjob.yaml`
4. **Resources**: Adjust CPU and memory requests/limits in job templates

## Customization

For on-premises Kubernetes clusters:

1. Ensure your storage class supports ReadWriteMany access mode
2. Adjust resource requests to match your node capabilities
3. Remove cloud-specific configurations if not needed
```

## Deployment Steps

Here are the commands to deploy the ETL pipeline:

```bash
# Create namespace
kubectl create namespace etl-pipeline
```

```bash
# Create PVC
kubectl apply -f etl-pipeline/kubernetes/etl-pvc.yaml
```

```bash
# Create ConfigMap with job templates
kubectl apply -f etl-pipeline/kubernetes/etl-configmap.yaml
```

```bash
# Create secrets for database credentials
kubectl create secret generic db-credentials \
  --from-literal=connection-string='postgresql://user:password@postgres-host:5432/db' \
  -n etl-pipeline
```

```bash
# Create secrets for BigQuery credentials
kubectl create secret generic gcp-credentials \
  --from-file=service-account-json=/path/to/service-account.json \
  -n etl-pipeline
```

```bash
# Deploy controller CronJob
kubectl apply -f etl-pipeline/kubernetes/controller-cronjob.yaml