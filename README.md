# Local Spark MinIO Lakehouse

This project is a production-like local data platform built on Kubernetes for learning modern Data Engineering concepts.

The goal is to simulate a real lakehouse-oriented architecture locally using Kubernetes, MinIO, Spark on Kubernetes, Parquet-based lakehouse layers, and later Apache Iceberg, Kafka, Spark Operator, and an orchestration API.

The project is intentionally designed as a learning-oriented but production-like system. It is not only a simple local demo; each component is structured to reflect how real data platforms separate storage, compute, orchestration, and platform operators.

---

## Current Architecture

The current phase focuses on the object storage layer.

```text
Kubernetes Cluster
│
├── lakehouse
│   ├── MinIO StatefulSet
│   ├── MinIO Headless Service
│   ├── MinIO Client-facing Service
│   ├── PersistentVolumeClaims
│   └── Bucket bootstrap Job
│
├── spark-operator
│   └── Future Spark Operator controller
│
├── spark-jobs
│   └── Future SparkApplication, driver, and executor pods
│
└── orchestration
    └── Future FastAPI orchestration/control API
```

---

## Namespace Design

The project separates platform responsibilities into multiple namespaces.

| Namespace | Purpose |
|---|---|
| `lakehouse` | Storage layer: MinIO, buckets, PVCs, and future lakehouse storage components |
| `spark-operator` | Spark Operator controller namespace |
| `spark-jobs` | SparkApplication resources, Spark driver pods, and executor pods |
| `orchestration` | Future orchestration API for triggering and tracking Spark pipelines |

This separation is intentional. It keeps storage, compute, operator, and orchestration responsibilities isolated, similar to a real production platform.

---

## Current Kubernetes Resources

Current files:

```text
k8s/
├── minio/
│   ├── job-create-bucket.yaml
│   ├── secret.yaml
│   ├── service-headless.yaml
│   ├── service.yaml
│   └── statefulset.yaml
│
└── namespaces/
    ├── lakehouse.yaml
    ├── orchestration.yaml
    ├── spark-jobs.yaml
    └── spark-operator.yaml
```

---

## Current Status

Completed:

- Kubernetes namespace layout
- Production-like MinIO object storage layer
- StatefulSet-based MinIO deployment
- 4 MinIO pods with 4 PVCs
- Headless service for internal pod discovery
- ClusterIP service for client access
- Bucket bootstrap job
- Lakehouse bucket structure

Next steps:

- Install Spark Operator using Helm
- Create Spark job namespace and RBAC
- Submit the first SparkApplication
- Write sample data to MinIO as Parquet
- Build raw → bronze → silver → gold Spark pipeline
- Add orchestration API for triggering Spark jobs

---

## Future Target Architecture

```text
Client / API caller
        ↓
orchestration namespace
        ↓
FastAPI job trigger API
        ↓
spark-jobs namespace
        ↓
SparkApplication
        ↓
Spark Driver Pod
        ↓
Spark Executor Pods
        ↓
lakehouse namespace
        ↓
MinIO datalake bucket
        ↓
raw / bronze / silver / gold
```

---

## Learning Goals

This project is designed to teach:

- Kubernetes namespaces and resource separation
- StatefulSet vs Deployment
- Headless Service vs normal Service
- PVC per pod pattern
- S3-compatible object storage with MinIO
- Lakehouse storage layout
- Spark driver/executor architecture
- Spark on Kubernetes
- Spark Operator and SparkApplication
- Production-like orchestration and job triggering
- Data platform design principles

---

## Notes

This is a local learning project. It uses production-like patterns, but it is not a production-ready deployment.

For example, MinIO runs with multiple pods and PVCs, but all resources are still backed by the local Docker Desktop Kubernetes environment. Therefore, it should be understood as a production architecture simulation rather than a real HA storage system.
