# MinIO Object Storage Layer

This directory contains the Kubernetes manifests for the MinIO object storage layer of the `local-spark-minio-lakehouse` project.

MinIO is used as a local S3-compatible object storage system. It simulates the storage layer that Spark will later use to read and write lakehouse data.

---

## Purpose

The MinIO layer provides:

- S3-compatible object storage
- A `datalake` bucket
- Lakehouse-style prefixes: `raw`, `bronze`, `silver`, `gold`
- Persistent storage through Kubernetes PVCs
- Stable pod identity using StatefulSet and Headless Service
- A client-facing Kubernetes Service for Spark and bootstrap jobs

This layer is intentionally designed to be more production-like than a simple single-pod MinIO demo.

---

## Files

```text
k8s/minio/
├── job-create-bucket.yaml
├── secret.yaml
├── service-headless.yaml
├── service.yaml
└── statefulset.yaml
```

---

## Resource Overview

### `statefulset.yaml`

Creates a 4-pod MinIO StatefulSet:

```text
minio-0
minio-1
minio-2
minio-3
```

Each pod gets its own PVC:

```text
minio-0 → data-minio-0
minio-1 → data-minio-1
minio-2 → data-minio-2
minio-3 → data-minio-3
```

This simulates a distributed object storage pattern where each storage node owns its own disk.

---

### `service-headless.yaml`

Creates the `minio-headless` service.

Purpose:

- Gives MinIO pods stable DNS identities
- Allows MinIO pods to discover each other
- Supports StatefulSet networking

Example DNS names:

```text
minio-0.minio-headless.lakehouse.svc.cluster.local
minio-1.minio-headless.lakehouse.svc.cluster.local
minio-2.minio-headless.lakehouse.svc.cluster.local
minio-3.minio-headless.lakehouse.svc.cluster.local
```

This service is mainly for internal MinIO cluster communication.

---

### `service.yaml`

Creates the normal client-facing `minio` service.

Purpose:

- Provides a single endpoint for Spark jobs, bootstrap jobs, and local port-forwarding
- Hides individual MinIO pod addresses from clients

Client endpoint inside the cluster:

```text
http://minio.lakehouse.svc.cluster.local:9000
```

If used from the same namespace:

```text
http://minio:9000
```

Spark jobs will later use this service as the S3A endpoint.

---

### `secret.yaml`

Stores MinIO credentials as Kubernetes Secret values.

Expected keys:

```text
MINIO_ROOT_USER
MINIO_ROOT_PASSWORD
```

In a real project, secret values should not be committed to Git. A safer pattern is to commit a `secret.example.yaml` file and keep the real `secret.yaml` ignored by Git.

---

### `job-create-bucket.yaml`

Creates a one-time Kubernetes Job that bootstraps the bucket layout.

The Job uses the MinIO Client image:

```text
minio/mc
```

It does the following:

1. Waits until MinIO is reachable
2. Creates the `datalake` bucket if it does not already exist
3. Creates visible lakehouse layer prefixes using `.keep` objects

Created layout:

```text
datalake/
├── raw/
├── bronze/
├── silver/
├── gold/
├── checkpoints/
├── audit/
└── metadata/
```

Object storage does not have real folders. The `.keep` files are marker objects that make these prefixes visible before Spark writes real data.

---

## Deployment Order

Recommended apply order:

```bash
kubectl apply -f k8s/namespaces/lakehouse.yaml
kubectl apply -f k8s/minio/secret.yaml
kubectl apply -f k8s/minio/service-headless.yaml
kubectl apply -f k8s/minio/service.yaml
kubectl apply -f k8s/minio/statefulset.yaml
kubectl apply -f k8s/minio/job-create-bucket.yaml
```

---

## Validation Commands

### Check MinIO pods

```bash
kubectl get pods -n lakehouse
```

Expected:

```text
minio-0   1/1   Running
minio-1   1/1   Running
minio-2   1/1   Running
minio-3   1/1   Running
```

---

### Check PVCs

```bash
kubectl get pvc -n lakehouse
```

Expected:

```text
data-minio-0   Bound
data-minio-1   Bound
data-minio-2   Bound
data-minio-3   Bound
```

---

### Check services

```bash
kubectl get svc -n lakehouse
```

Expected services:

```text
minio
minio-headless
```

---

### Check bucket bootstrap job

```bash
kubectl get jobs -n lakehouse
kubectl logs job/minio-create-bucket -n lakehouse
```

Expected log output includes:

```text
Bucket created successfully `local/datalake`.
Bucket bootstrap completed.
```

---

## Accessing the MinIO Console

Run:

```bash
kubectl port-forward svc/minio -n lakehouse 9001:9001
```

Open:

```text
http://localhost:9001
```

Use the credentials from `secret.yaml`.

---

## Architecture Summary

```text
Spark / Future clients
        ↓
minio Service
        ↓
MinIO distributed-like StatefulSet
        ↓
Per-pod PVCs
        ↓
datalake bucket
        ↓
raw / bronze / silver / gold
```

Internal MinIO discovery:

```text
minio-0 ↔ minio-1 ↔ minio-2 ↔ minio-3
        via minio-headless service
```

Client access:

```text
Spark / bootstrap job / future API
        ↓
http://minio.lakehouse.svc.cluster.local:9000
```

---

## Important Concepts Learned

This MinIO layer demonstrates:

- StatefulSet for stateful workloads
- Stable pod identity
- Headless Service
- Client-facing ClusterIP Service
- Per-pod PVC pattern
- Kubernetes Secret usage
- Bootstrap Job pattern
- S3-compatible object storage
- Bucket vs prefix distinction
- Lakehouse storage layout

---

## Notes

This setup is production-like, but it is not production-grade.

All pods and PVCs run on a local Docker Desktop Kubernetes cluster. Therefore, it does not provide real high availability. The purpose is to simulate production patterns locally for learning.
