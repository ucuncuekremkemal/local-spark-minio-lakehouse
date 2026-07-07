# Binance Collector

This application is a lightweight Python WebSocket collector that listens to Binance public market data and writes raw events into MinIO as JSONL files.

It is the ingestion component of the local financial market lakehouse project.

## Purpose

The collector is responsible only for capturing raw market events and storing them as close to the source format as possible.

Current stream:

```text
btcusdt@kline_1m
```

Current flow:

```text
Binance WebSocket
    ↓
Python collector
    ↓
MinIO raw JSONL files
```

## Why JSONL?

The raw layer should preserve the original source payload with minimal transformation.

For that reason, the collector writes newline-delimited JSON files instead of Parquet.

Parquet conversion and schema normalization are handled later by Spark jobs in the bronze layer.

## Output Path

The collector writes files to MinIO using the following layout:

```text
s3://datalake/raw/market/binance/klines_1m/
  symbol=BTCUSDT/
    date=YYYY-MM-DD/
      hour=HH/
        part-YYYYMMDDTHHMMSS-<uuid>.jsonl
```

## Configuration

| Variable | Description | Example |
|---|---|---|
| `MINIO_ENDPOINT` | MinIO S3-compatible endpoint | `http://minio.lakehouse.svc.cluster.local:9000` |
| `MINIO_ACCESS_KEY` | MinIO access key | From Kubernetes Secret |
| `MINIO_SECRET_KEY` | MinIO secret key | From Kubernetes Secret |
| `MINIO_BUCKET` | Target bucket | `datalake` |
| `FLUSH_INTERVAL_SECONDS` | Flush interval in seconds | `300` |
| `PYTHONUNBUFFERED` | Forces immediate container logs | `1` |

## Flush Behavior

The collector keeps incoming WebSocket messages in memory and writes them to MinIO every 5 minutes by default.

```text
FLUSH_INTERVAL_SECONDS=300
```

This reduces the number of small files while still keeping data reasonably fresh during local development.

## Reconnect Behavior

If the WebSocket connection fails or closes, the collector:

1. Flushes the current in-memory buffer to MinIO.
2. Clears the buffer.
3. Waits for a short delay.
4. Reconnects to Binance WebSocket.

## Local Docker Build

Build the collector image from the project root:

```bash
docker build -t binance-collector:1.0.0 -f apps/binance-collector/Dockerfile apps/binance-collector
```

For cleaner versioning, increment the image tag when code changes:

```bash
docker build -t binance-collector:1.0.1 -f apps/binance-collector/Dockerfile apps/binance-collector
```

## Requirements

Expected Python dependencies:

```text
boto3
websockets
```

## Logs

Expected logs:

```text
Connecting to Binance WebSocket: ...
Connected to Binance WebSocket
Flushed ... records to s3://datalake/raw/market/binance/klines_1m/...
```

`PYTHONUNBUFFERED=1` and `flush=True` are used so logs are visible immediately through Kubernetes logs.

## Notes

This collector is intentionally simple. It does not perform schema normalization, deduplication, candle validation, or Parquet conversion.

Those responsibilities belong to downstream Spark jobs.
