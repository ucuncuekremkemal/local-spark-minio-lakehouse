# Kubernetes Schedules

This directory contains Kubernetes scheduling resources for the lakehouse pipeline.

## Raw to Bronze Klines CronJob

File:

```text
raw-to-bronze-klines-cronjob.yaml
```

This CronJob runs every hour and dynamically creates a SparkApplication for the previous UTC hour.

## Why a CronJob?

The Spark raw-to-bronze job is a recurring batch process.

Instead of manually applying a static SparkApplication YAML, the CronJob acts as a lightweight scheduler.

The CronJob:

1. calculates the previous UTC hour
2. builds a unique SparkApplication name
3. injects START_DATE, START_HOUR, END_DATE, and END_HOUR
4. submits the SparkApplication with kubectl
5. lets the Spark Operator run the actual Spark job

## Processing Window

The Spark job uses an end-exclusive time window:

```text
[START, END)
```

Example:

If the CronJob runs at:

```text
2026-07-08 14:05 UTC
```

it submits:

```text
START_DATE=2026-07-08
START_HOUR=13

END_DATE=2026-07-08
END_HOUR=14
```

This processes only:

```text
2026-07-08 hour=13
```

## Dynamic SparkApplication Names

Each hourly run gets a unique SparkApplication name:

```text
raw-to-bronze-klines-YYYYMMDDHH
```

Example:

```text
raw-to-bronze-klines-2026070813
```

This makes driver pods and run history easier to inspect.

## Manual Trigger

```bash
kubectl create job \
  --from=cronjob/raw-to-bronze-klines-cronjob \
  raw-to-bronze-klines-manual-001 \
  -n spark-jobs
```

## Check Resources

```bash
kubectl get cronjobs -n spark-jobs
kubectl get jobs -n spark-jobs
kubectl get sparkapplications -n spark-jobs
kubectl get pods -n spark-jobs
```

## Logs

Scheduler job logs:

```bash
kubectl logs job/<scheduler-job-name> -n spark-jobs
```

Spark driver logs:

```bash
kubectl logs -f <spark-driver-pod-name> -n spark-jobs
```

## Notes

The CronJob uses UTC because raw data is partitioned by UTC date and hour.

The SparkApplication uses dynamic partition overwrite to avoid deleting unrelated Bronze partitions.
