# Spark Operator Layer

This directory documents the Spark Operator setup used in the `local-spark-minio-lakehouse` project.

The goal of this layer is to run Apache Spark jobs on Kubernetes in a production-like, Kubernetes-native way.

Instead of running a permanent Spark master/worker cluster, this project uses a Spark Operator model:

```text
SparkApplication
        ↓
Spark Operator
        ↓
Driver Pod
        ↓
Executor Pod(s)
```

This means Spark compute is created only when a job is submitted and released after the job finishes.

---

## Why Spark Operator?

Kubernetes does not natively understand Spark jobs.

By default, Kubernetes knows resources such as:

```text
Pod
Service
Deployment
StatefulSet
Job
ConfigMap
Secret
PVC
Namespace
```

Spark Operator extends Kubernetes with a new custom resource:

```text
SparkApplication
```

This allows Spark jobs to be described declaratively using YAML.

Instead of manually running `spark-submit`, we can define a Spark job as a Kubernetes resource.

---

## Operator Choice

During the setup, two different Spark Operator options were evaluated.

### Apache Spark Kubernetes Operator

The Apache Spark Kubernetes Operator was installed first.

It created CRDs such as:

```text
sparkapplications.spark.apache.org
sparkclusters.spark.apache.org
```

However, its `SparkApplication` format was more `spark-submit`-style and less readable for the learning goals of this project.

Example style:

```yaml
spec:
  mainClass: ...
  jars: ...
  sparkConf:
    spark.kubernetes.container.image: ...
    spark.executor.instances: ...
```

This format is useful, but it was less suitable for a PySpark-focused learning project.

### Kubeflow Spark Operator

The project then switched to the Kubeflow Spark Operator.

It provides a more readable `SparkApplication` format:

```yaml
spec:
  type: Python
  mode: cluster
  image: ...
  mainApplicationFile: ...
  driver:
    cores: ...
    memory: ...
  executor:
    instances: ...
    cores: ...
    memory: ...
```

This format makes driver, executor, image, and application file settings easier to understand.

For this reason, the project uses the Kubeflow Spark Operator.

---

## Namespace Design

Spark-related responsibilities are separated across namespaces.

```text
spark-operator
    → Spark Operator controller and webhook

spark-jobs
    → SparkApplication resources, driver pods, executor pods
```

The operator runs in:

```text
spark-operator
```

Spark jobs run in:

```text
spark-jobs
```

This separation keeps the platform component separate from user workloads.

---

## Helm Installation

The Kubeflow Spark Operator is installed with Helm.

Helm repository:

```bash
helm repo add spark-operator https://kubeflow.github.io/spark-operator
helm repo update
```

Chart:

```text
spark-operator/spark-operator
```

The operator is installed into the `spark-operator` namespace.

Example install command:

```bash
helm install spark-operator spark-operator/spark-operator \
  --namespace spark-operator \
  -f spark-operator/values.yaml
```

If running from the repository root, use:

```bash
helm install spark-operator spark-operator/spark-operator \
  --namespace spark-operator \
  -f k8s/spark-operator/values.yaml
```

---

## Helm Values

The important configuration is:

```yaml
spark:
  jobNamespaces:
    - spark-jobs

  serviceAccount:
    create: true
    name: spark

controller:
  replicas: 1

webhook:
  enable: true
```

Meaning:

| Setting | Meaning |
|---|---|
| `spark.jobNamespaces` | Namespaces where SparkApplication resources are watched |
| `spark.serviceAccount.name` | ServiceAccount used by Spark driver pods |
| `controller.replicas` | Number of Spark Operator controller replicas |
| `webhook.enable` | Enables the admission webhook |

The most important part is:

```yaml
spark:
  jobNamespaces:
    - spark-jobs
```

This tells the operator to watch the `spark-jobs` namespace for Spark jobs.

---

## Controller and Webhook

After installation, the operator creates two main pods:

```text
spark-operator-controller
spark-operator-webhook
```

### Controller

The controller is the main brain of the operator.

It watches `SparkApplication` resources and creates the required driver and executor pods.

Flow:

```text
SparkApplication created
        ↓
Controller detects it
        ↓
Driver pod is created
        ↓
Executor pod(s) are created
        ↓
Spark job status is updated
```

Without the controller, a `SparkApplication` would only be a passive Kubernetes object.

### Webhook

The webhook participates in the Kubernetes admission process.

It can validate or mutate SparkApplication resources before they are accepted by the Kubernetes API server.

Typical responsibilities:

```text
Validate SparkApplication YAML
Apply default values
Reject invalid specs
Help keep Spark resources consistent
```

---

## CRDs

After installing the Kubeflow Spark Operator, the following CRDs are expected:

```text
sparkapplications.sparkoperator.k8s.io
scheduledsparkapplications.sparkoperator.k8s.io
sparkconnects.sparkoperator.k8s.io
```

Check with:

```bash
kubectl get crd | grep spark
```

The most important CRD for this project is:

```text
sparkapplications.sparkoperator.k8s.io
```

This enables the following command:

```bash
kubectl get sparkapplications -n spark-jobs
```

---

## First SparkApplication

The first Spark job was created as:

```text
k8s/spark-applications/first-spark-job.yaml
```

Example:

```yaml
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: first-spark-job
  namespace: spark-jobs
spec:
  type: Python
  mode: cluster

  image: apache/spark:3.5.3
  imagePullPolicy: IfNotPresent

  mainApplicationFile: local:///opt/spark/examples/src/main/python/pi.py
  sparkVersion: 3.5.3

  restartPolicy:
    type: Never

  driver:
    cores: 1
    memory: 512m
    serviceAccount: spark
    labels:
      version: 3.5.3

  executor:
    cores: 1
    instances: 1
    memory: 512m
    labels:
      version: 3.5.3
```

This job uses the built-in Spark Python Pi example from the Spark Docker image.

The important line is:

```yaml
mainApplicationFile: local:///opt/spark/examples/src/main/python/pi.py
```

`local://` means the file already exists inside the container image.

---

## First Job Result

The first SparkApplication successfully ran on Kubernetes.

Observed pod lifecycle:

```text
first-spark-job-driver                 Running
pythonpi-...-exec-1                    Pending
pythonpi-...-exec-1                    ContainerCreating
pythonpi-...-exec-1                    Running
pythonpi-...-exec-1                    Completed
first-spark-job-driver                 Completed
```

Driver log contained:

```text
Pi is roughly 3.140440
```

This confirms that:

```text
SparkApplication was created
Spark Operator detected it
Driver pod was created
Executor pod was created
Spark tasks ran on the executor
The job completed successfully
```

---



## Useful Commands

### Check operator pods

```bash
kubectl get pods -n spark-operator
```

Expected:

```text
spark-operator-controller-...   1/1   Running
spark-operator-webhook-...      1/1   Running
```

### Check Spark CRDs

```bash
kubectl get crd | grep spark
```

Expected:

```text
sparkapplications.sparkoperator.k8s.io
scheduledsparkapplications.sparkoperator.k8s.io
sparkconnects.sparkoperator.k8s.io
```

### Check SparkApplications

```bash
kubectl get sparkapplications -n spark-jobs
```

### Apply the first Spark job

```bash
kubectl apply -f spark-applications/first-spark-job.yaml
```

If running from the repository root:

```bash
kubectl apply -f k8s/spark-applications/first-spark-job.yaml
```

### Watch Spark pods

```bash
kubectl get pods -n spark-jobs -w
```

### Read driver logs

```bash
kubectl logs first-spark-job-driver -n spark-jobs
```

---

## Current Status

Completed:

- Apache Spark Kubernetes Operator was evaluated and removed
- Kubeflow Spark Operator was installed
- Spark Operator controller and webhook were created
- SparkApplication CRD was installed
- `spark-jobs` namespace was configured as the Spark job namespace
- Spark service account exists in `spark-jobs`
- First SparkApplication successfully ran
- Driver and executor pod lifecycle was observed
- Driver logs confirmed successful Spark execution

---

## Next Steps

The next phase is to move from the built-in Spark example to custom PySpark code.

Planned next steps:

1. Create a custom PySpark script
2. Mount it into the driver pod using a ConfigMap
3. Run it as a SparkApplication
4. Then connect Spark to MinIO
5. Write Parquet data to the `datalake` bucket
6. Build raw → bronze → silver → gold transformations

Target flow:

```text
Custom PySpark file
        ↓
ConfigMap
        ↓
SparkApplication
        ↓
Driver Pod
        ↓
Executor Pod
        ↓
MinIO datalake bucket
```

---

## Key Learning Points

This layer demonstrates:

- Helm-based platform component installation
- Spark Operator architecture
- Kubernetes CRDs
- Controller and webhook pattern
- SparkApplication lifecycle
- Driver and executor pod creation
- Kubernetes-native Spark execution
- Image pull troubleshooting
- Difference between platform operator and Spark workload namespaces
