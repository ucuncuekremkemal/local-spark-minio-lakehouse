# Local Spark MinIO Lakehouse

This project is a production-like local data platform built on Kubernetes for learning modern Data Engineering and lakehouse architecture concepts.

The goal is to simulate a real Spark-on-Kubernetes lakehouse environment locally using Kubernetes, MinIO, Spark Operator, custom Spark Docker images, and Parquet-based lakehouse layers. Later phases may include Apache Iceberg, Kafka, and an orchestration API.

The project is intentionally designed as a learning-oriented but production-like system. It is not just a simple local demo. Each component is structured to reflect how real data platforms separate storage, compute, platform operators, job execution, and orchestration.

---

## Architecture Overview

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
│   ├── Spark Operator controller
│   └── Spark Operator webhook
│
├── spark-jobs
│   ├── SparkApplication resources
│   ├── Spark driver pods
│   └── Spark executor pods
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
| `spark-operator` | Spark Operator controller and webhook namespace |
| `spark-jobs` | SparkApplication resources, Spark driver pods, and Spark executor pods |
| `orchestration` | Future orchestration API for triggering and tracking Spark pipelines |

This separation is intentional. It keeps storage, compute, operator, and orchestration responsibilities isolated, similar to a real production platform.

---

## Repository Structure

```text
local-spark-minio-lakehouse/
├── README.md
├── docker/
│   └── spark/
│       └── Dockerfile
├── k8s/
│   ├── namespaces/
│   │   ├── lakehouse.yaml
│   │   ├── orchestration.yaml
│   │   ├── spark-jobs.yaml
│   │   └── spark-operator.yaml
│   ├── minio/
│   │   ├── README.md
│   │   ├── secret.example.yaml
│   │   ├── secret.yaml
│   │   ├── service-headless.yaml
│   │   ├── service.yaml
│   │   ├── statefulset.yaml
│   │   └── job-create-bucket.yaml
│   ├── spark-operator/
│   │   ├── README.md
│   │   └── values.yaml
│   └── spark-applications/
│       ├── examples/
│       │   ├── first-spark-job.yaml
│       │   ├── hello-spark-configmap.yaml
│       │   └── hello-spark-image.yaml
│       └── jobs/
│           └── minio-write-test.yaml
├── spark/
│   ├── jobs/
│   │   ├── hello_spark.py
│   │   └── minio_write_test.py
│   └── common/
│       └── __init__.py
└── docs/
    ├── architecture.md
    ├── minio.md
    ├── spark-operator.md
    └── development-flow.md
```

The structure separates Kubernetes manifests, Spark application code, Docker image definitions, and documentation.

---

## Core Components

### MinIO Lakehouse Storage

MinIO is used as the local S3-compatible object storage layer.

The current MinIO setup includes:

- `StatefulSet`-based deployment
- 4 MinIO pods
- 4 PVCs, one per pod
- Headless service for stable internal pod discovery
- ClusterIP service for client access
- Bootstrap job for bucket and prefix creation

The main bucket is:

```text
datalake
```

The initial lakehouse layout is:

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

---

### Spark Operator

Spark jobs are submitted through Spark Operator using `SparkApplication` resources.

The operator is installed in the `spark-operator` namespace and watches Spark jobs in the `spark-jobs` namespace.

The current Spark job execution model is:

```text
SparkApplication YAML
        ↓
Spark Operator
        ↓
Driver pod
        ↓
Executor pod(s)
```

This project uses the Kubeflow Spark Operator style `SparkApplication` API.

---

### Custom Spark Image

Spark application code is packaged into a custom Docker image.

Current image pattern:

```text
spark/jobs/*.py
        ↓
Docker build
        ↓
local-spark-jobs:<tag>
        ↓
SparkApplication image field
        ↓
Driver and executor pods
```

The Dockerfile lives under:

```text
docker/spark/Dockerfile
```

Example build command:

```bash
docker build -t local-spark-jobs:0.1.0 -f docker/spark/Dockerfile .
```

SparkApplication then references the image:

```yaml
image: local-spark-jobs:0.1.0
imagePullPolicy: IfNotPresent
```

This is the primary development model going forward. ConfigMap-based code mounting was tested for learning volume mounts, but custom images are the preferred pattern for application packaging.

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
- Spark Operator installation
- Spark job namespace setup
- First SparkApplication execution
- ConfigMap-based code mount test
- Custom Spark image build and execution
- PySpark job execution with driver and executor pods

Next steps:

- Cleanly organize example SparkApplication manifests
- Add a MinIO write test Spark job
- Write sample data to MinIO as Parquet
- Build raw → bronze → silver → gold Spark pipeline
- Add shared Spark utilities under `spark/common`
- Add orchestration API for triggering Spark jobs
- Later evaluate Iceberg and Kafka integration

---

## Local Development Flow

The main development loop is:

```text
Edit PySpark code
        ↓
Build custom Spark image
        ↓
Update SparkApplication image tag if needed
        ↓
Apply SparkApplication YAML
        ↓
Inspect driver logs and SparkApplication status
```

Common commands:

```bash
# Build Spark image
docker build -t local-spark-jobs:0.1.0 -f docker/spark/Dockerfile .

# Submit a SparkApplication
kubectl apply -f k8s/spark-applications/jobs/<job-name>.yaml

# Watch pods
kubectl get pods -n spark-jobs -w

# Read driver logs
kubectl logs <driver-pod-name> -n spark-jobs

# Check SparkApplications
kubectl get sparkapplications -n spark-jobs
```

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

This project is designed to teach and practice:

- Kubernetes namespaces and resource separation
- StatefulSet vs Deployment
- Headless Service vs normal Service
- PVC per pod pattern
- S3-compatible object storage with MinIO
- Lakehouse storage layout
- Spark driver/executor architecture
- Spark on Kubernetes
- Spark Operator and SparkApplication resources
- Custom Spark Docker image packaging
- PySpark job parameterization
- Parquet writes to object storage
- Raw, bronze, silver, and gold lakehouse layering
- Spark resource tuning
- Production-like orchestration and job triggering
- Data platform design principles

---

## Notes

This is a local learning project. It uses production-like patterns, but it is not a production-ready deployment.

For example, MinIO runs with multiple pods and PVCs, but all resources are still backed by the local Docker Desktop Kubernetes environment. Therefore, it should be understood as a production architecture simulation rather than a real highly available storage system.

Secrets should not be committed to Git. Keep real secret files such as `k8s/minio/secret.yaml` local, and commit only example files such as `secret.example.yaml`.
