# Ingestion Kubernetes Manifests

This directory contains Kubernetes manifests for ingestion workloads.

Currently, it deploys the Binance Collector, which reads live market data from Binance WebSocket and writes raw JSONL files into MinIO.

## Namespace

The collector runs in the `ingestion` namespace.

Create the namespace if it does not exist:

```bash
kubectl create namespace ingestion
```

## Components

Current manifest:

```text
binance-collector-deployment.yaml
```

This creates a long-running Kubernetes Deployment.

## Why Deployment?

The Binance collector is a continuously running WebSocket consumer.

For that reason, it should run as a Deployment instead of a Job.

```text
Deployment = long-running service
Job        = finite task
```

## Architecture

```text
Binance WebSocket
    ↓
binance-collector Deployment
    ↓
MinIO Service in lakehouse namespace
    ↓
datalake bucket
    ↓
raw/market/binance/klines_1m/
```

## Deployment

Apply the manifest:

```bash
kubectl apply -f k8s/ingestion/binance-collector-deployment.yaml
```

Check rollout status:

```bash
kubectl rollout status deployment/binance-collector-deployment -n ingestion
```

Check pod status:

```bash
kubectl get pods -n ingestion
```

## Restarting the Collector

```bash
kubectl rollout restart deployment/binance-collector-deployment -n ingestion
kubectl rollout status deployment/binance-collector-deployment -n ingestion
```

## Logs

Follow logs:

```bash
kubectl logs -f deployment/binance-collector-deployment -n ingestion
```

Show recent logs:

```bash
kubectl logs deployment/binance-collector-deployment -n ingestion --tail=100
```

## MinIO Connection

The collector connects to MinIO through the internal Kubernetes service DNS:

```text
http://minio.lakehouse.svc.cluster.local:9000
```

This works because MinIO runs in the `lakehouse` namespace.

## Required Secret

The collector expects a Kubernetes Secret named `minio-secret` in the `ingestion` namespace.

The Deployment reads:

```text
MINIO_ROOT_USER
MINIO_ROOT_PASSWORD
```

and maps them to:

```text
MINIO_ACCESS_KEY
MINIO_SECRET_KEY
```

Check the Secret:

```bash
kubectl get secret minio-secret -n ingestion
```

## Environment Variables

| Variable | Description |
|---|---|
| `PYTHONUNBUFFERED` | Makes Python logs visible immediately |
| `MINIO_ACCESS_KEY` | MinIO access key from Kubernetes Secret |
| `MINIO_SECRET_KEY` | MinIO secret key from Kubernetes Secret |
| `MINIO_ENDPOINT` | Internal MinIO service endpoint |
| `MINIO_BUCKET` | Target bucket name |
| `FLUSH_INTERVAL_SECONDS` | Time-based flush interval |

## Current Settings

```text
replicas: 1
FLUSH_INTERVAL_SECONDS: 300
MINIO_BUCKET: datalake
```

`replicas` is intentionally set to `1`.

Running multiple replicas against the same Binance stream would produce duplicate raw events unless downstream deduplication is explicitly handled.

## Image Build

The Deployment uses a local Docker image:

```text
binance-collector:1.0.0
```

Build it from the project root:

```bash
docker build -t binance-collector:1.0.0 -f apps/binance-collector/Dockerfile apps/binance-collector
```

If code changes, rebuild the image and restart the Deployment:

```bash
docker build -t binance-collector:1.0.0 -f apps/binance-collector/Dockerfile apps/binance-collector
kubectl rollout restart deployment/binance-collector-deployment -n ingestion
```

## Verifying Data in MinIO

Port-forward the MinIO Console:

```bash
kubectl port-forward svc/minio -n lakehouse 9001:9001
```

Open:

```text
http://localhost:9001
```

Then check:

```text
datalake/raw/market/binance/klines_1m/
```

A healthy collector should create new JSONL files approximately every 5 minutes.

## Troubleshooting

Check pod status:

```bash
kubectl get pods -n ingestion
```

Describe the pod:

```bash
kubectl describe pod <pod-name> -n ingestion
```

Check logs:

```bash
kubectl logs deployment/binance-collector-deployment -n ingestion --tail=100
```

Check environment variables inside the pod:

```bash
kubectl exec -it <pod-name> -n ingestion -- env | grep MINIO
```

Common problems:

| Symptom | Possible Cause |
|---|---|
| No new files in MinIO | WebSocket connection issue, MinIO endpoint issue, bucket issue |
| `AccessDenied` | Wrong MinIO credentials |
| `NoSuchBucket` | Bucket does not exist |
| `Connection refused` | Wrong MinIO service endpoint |
| No logs | Missing `PYTHONUNBUFFERED=1` or print buffering |
| `ImagePullBackOff` | Local image tag not found or image pull policy issue |

## Next Step

The next layer is a Spark job that reads raw JSONL files and writes normalized bronze Parquet data.

```text
raw JSONL
    ↓
SparkApplication
    ↓
bronze Parquet
```
