import asyncio
import json
import os
import ssl
from datetime import datetime, timezone
from uuid import uuid4

import boto3
import websockets
from websockets.exceptions import ConnectionClosed


BINANCE_WS_URL = "wss://data-stream.binance.vision/ws/btcusdt@kline_1m"
RECONNECT_DELAY_SECONDS = int(os.getenv("RECONNECT_DELAY_SECONDS", "5"))
FLUSH_INTERVAL_SECONDS = int(os.getenv("FLUSH_INTERVAL_SECONDS", "300"))
MESSAGE_TIMEOUT_SECONDS = int(os.getenv("MESSAGE_TIMEOUT_SECONDS", "90"))


def log(message: str) -> None:
    print(f"{utc_now().isoformat()} | {message}", flush=True)


def get_required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def get_boto3_client():
    endpoint_url = get_required_env("MINIO_ENDPOINT")
    aws_access_key_id = get_required_env("MINIO_ACCESS_KEY")
    aws_secret_access_key = get_required_env("MINIO_SECRET_KEY")
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=os.getenv("AWS_REGION", "us-east-1"),
    )


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def build_s3_key(symbol: str, event_time: datetime) -> str:
    date_partition = event_time.strftime("%Y-%m-%d")
    hour_partition = event_time.strftime("%H")
    file_name = f"part-{event_time.strftime('%Y%m%dT%H%M%S')}-{uuid4().hex}.jsonl"

    return (
        "raw/market/binance/klines_1m/"
        f"symbol={symbol}/"
        f"date={date_partition}/"
        f"hour={hour_partition}/"
        f"{file_name}"
    )


def wrap_message(message: str) -> dict:
    ingestion_time = utc_now()
    payload = json.loads(message)

    return {
        "ingestion_time": ingestion_time.isoformat(),
        "source_system": "binance",
        "stream": "btcusdt@kline_1m",
        "payload": payload,
    }


def flush_buffer_to_minio(s3, bucket: str, buffer: list[dict], reason: str) -> None:
    if not buffer:
        return

    symbol = buffer[0]["payload"].get("s", "UNKNOWN")
    event_time = utc_now()
    object_key = build_s3_key(symbol=symbol, event_time=event_time)

    body = "\n".join(json.dumps(record, separators=(",", ":")) for record in buffer) + "\n"

    s3.put_object(
        Bucket=bucket,
        Key=object_key,
        Body=body.encode("utf-8"),
        ContentType="application/x-ndjson",
    )

    log(
        f"Flushed {len(buffer)} records to s3://{bucket}/{object_key} "
        f"reason={reason}"
    )


async def receive_messages(s3):
    ssl_context = ssl.create_default_context()

    bucket = get_required_env("MINIO_BUCKET")
    buffer: list[dict] = []
    last_flush_time = utc_now()
    last_message_time: datetime | None = None
    reconnect_count = 0

    while True:
        try:
            log(f"Connecting to Binance WebSocket: {BINANCE_WS_URL}")

            async with websockets.connect(
                BINANCE_WS_URL,
                ssl=ssl_context,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=10,
                open_timeout=20,
            ) as websocket:
                reconnect_count += 1
                log(f"Connected to Binance WebSocket connection_number={reconnect_count}")

                while True:
                    now = utc_now()
                    should_flush_by_time = (
                        now - last_flush_time
                    ).total_seconds() >= FLUSH_INTERVAL_SECONDS

                    if should_flush_by_time:
                        flush_buffer_to_minio(
                            s3=s3,
                            bucket=bucket,
                            buffer=buffer,
                            reason="time_interval",
                        )
                        buffer.clear()
                        last_flush_time = now

                    try:
                        message = await asyncio.wait_for(
                            websocket.recv(),
                            timeout=MESSAGE_TIMEOUT_SECONDS,
                        )
                    except asyncio.TimeoutError:
                        log(
                            f"No WebSocket message received for "
                            f"{MESSAGE_TIMEOUT_SECONDS} seconds; forcing reconnect"
                        )
                        flush_buffer_to_minio(
                            s3=s3,
                            bucket=bucket,
                            buffer=buffer,
                            reason="message_timeout",
                        )
                        buffer.clear()
                        last_flush_time = utc_now()
                        break

                    record = wrap_message(message)
                    buffer.append(record)
                    last_message_time = utc_now()

                    if len(buffer) == 1:
                        log(
                            f"Receiving messages; latest_symbol="
                            f"{record['payload'].get('s', 'UNKNOWN')}"
                        )

        except asyncio.CancelledError:
            log("Collector stopped by cancellation")
            flush_buffer_to_minio(
                s3=s3,
                bucket=bucket,
                buffer=buffer,
                reason="cancelled",
            )
            raise
        except ConnectionClosed as exc:
            log(
                f"WebSocket connection closed: code={exc.code} "
                f"reason={exc.reason!r} last_message_time={last_message_time}"
            )
            flush_buffer_to_minio(
                s3=s3,
                bucket=bucket,
                buffer=buffer,
                reason="connection_closed",
            )
            buffer.clear()
            last_flush_time = utc_now()
        except Exception as exc:
            log(
                f"WebSocket connection failed or collector error: "
                f"{type(exc).__name__}: {exc} last_message_time={last_message_time}"
            )
            flush_buffer_to_minio(
                s3=s3,
                bucket=bucket,
                buffer=buffer,
                reason="exception",
            )
            buffer.clear()
            last_flush_time = utc_now()

        log(f"Reconnecting in {RECONNECT_DELAY_SECONDS} seconds...")
        await asyncio.sleep(RECONNECT_DELAY_SECONDS)


if __name__ == "__main__":
    s3_client = get_boto3_client()

    try:
        asyncio.run(receive_messages(s3_client))
    except KeyboardInterrupt:
        log("Collector stopped manually")
