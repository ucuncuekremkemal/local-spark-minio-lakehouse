import os
from datetime import datetime, timedelta
from typing import Iterator, List, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


RAW_BASE_PATH = "s3a://datalake/raw/market/binance/klines_1m"
BRONZE_BASE_PATH = "s3a://datalake/bronze/market/binance/klines_1m"
SYMBOL = "BTCUSDT"
DATETIME_FORMAT = "%Y-%m-%d %H"


def get_required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def get_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("raw-to-bronze-klines")
        .getOrCreate()
    )
    return spark


def parse_hour_range() -> Tuple[datetime, datetime]:
    start_date = get_required_env("START_DATE")
    start_hour = get_required_env("START_HOUR").zfill(2)
    end_date = get_required_env("END_DATE")
    end_hour = get_required_env("END_HOUR").zfill(2)

    start_time = datetime.strptime(f"{start_date} {start_hour}", DATETIME_FORMAT)
    end_time = datetime.strptime(f"{end_date} {end_hour}", DATETIME_FORMAT)

    if start_time >= end_time:
        raise RuntimeError(
            f"Invalid processing range. START must be before END. "
            f"start_time={start_time}, end_time={end_time}"
        )

    return start_time, end_time


def iter_hour_range(start_time: datetime, end_time: datetime) -> Iterator[Tuple[str, str]]:
    current_time = start_time

    while current_time < end_time:
        process_date = current_time.strftime("%Y-%m-%d")
        process_hour = current_time.strftime("%H")
        yield process_date, process_hour
        current_time += timedelta(hours=1)


def build_partition_path(base_path: str, process_date: str, process_hour: str) -> str:
    return (
        f"{base_path}/"
        f"symbol={SYMBOL}/"
        f"date={process_date}/"
        f"hour={process_hour}"
    )


def path_exists(spark: SparkSession, path: str) -> bool:
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    uri = spark._jvm.java.net.URI.create(path)
    file_system = spark._jvm.org.apache.hadoop.fs.FileSystem.get(uri, hadoop_conf)
    hadoop_path = spark._jvm.org.apache.hadoop.fs.Path(path)
    return file_system.exists(hadoop_path)


def build_existing_raw_paths(
    spark: SparkSession,
    start_time: datetime,
    end_time: datetime,
) -> List[str]:
    raw_paths: List[str] = []

    for process_date, process_hour in iter_hour_range(start_time, end_time):
        input_path = build_partition_path(
            base_path=RAW_BASE_PATH,
            process_date=process_date,
            process_hour=process_hour,
        )

        if path_exists(spark=spark, path=input_path):
            print(f"Found raw partition: {input_path}", flush=True)
            raw_paths.append(input_path)
        else:
            print(
                f"Skipping missing raw partition: "
                f"date={process_date}, hour={process_hour}, path={input_path}",
                flush=True,
            )

    if not raw_paths:
        raise RuntimeError(
            f"No raw partitions found for range start={start_time}, end={end_time}"
        )

    return raw_paths


def read_raw_klines(spark: SparkSession, raw_paths: List[str]) -> DataFrame:
    print("Reading raw kline data from paths:", flush=True)
    for path in raw_paths:
        print(f"  - {path}", flush=True)

    return (
        spark.read
        .option("basePath", RAW_BASE_PATH)
        .json(raw_paths)
    )


def transform_to_bronze(df: DataFrame) -> DataFrame:
    return (
        df.withColumns({
            "ingestion_time": F.col("ingestion_time").cast("timestamp"),
            "symbol": F.col("payload.s"),
            "event_type": F.col("payload.e"),
            "event_time_ms": F.col("payload.E").cast("long"),
            "event_time": F.from_unixtime(F.col("payload.E") / 1000).cast("timestamp"),
            "kline_start_time_ms": F.col("payload.k.t").cast("long"),
            "kline_start_time": F.from_unixtime(F.col("payload.k.t") / 1000).cast("timestamp"),
            "kline_close_time_ms": F.col("payload.k.T").cast("long"),
            "kline_close_time": F.from_unixtime(F.col("payload.k.T") / 1000).cast("timestamp"),
            "interval": F.col("payload.k.i"),
            "open_price": F.col("payload.k.o").cast("decimal(20,8)"),
            "high_price": F.col("payload.k.h").cast("decimal(20,8)"),
            "low_price": F.col("payload.k.l").cast("decimal(20,8)"),
            "close_price": F.col("payload.k.c").cast("decimal(20,8)"),
            "base_volume": F.col("payload.k.v").cast("decimal(28,8)"),
            "quote_volume": F.col("payload.k.q").cast("decimal(28,8)"),
            "trade_count": F.col("payload.k.n").cast("long"),
            "is_closed": F.col("payload.k.x").cast("boolean"),
            "first_trade_id": F.col("payload.k.f").cast("long"),
            "last_trade_id": F.col("payload.k.L").cast("long"),
            "taker_buy_base_volume": F.col("payload.k.V").cast("decimal(28,8)"),
            "taker_buy_quote_volume": F.col("payload.k.Q").cast("decimal(28,8)"),
        })
        .withColumns({
            "date": F.date_format(F.col("kline_start_time"), "yyyy-MM-dd"),
            "hour": F.date_format(F.col("kline_start_time"), "HH"),
        })
        .drop("payload")
    )


def write_bronze_klines(df: DataFrame) -> None:
    print(f"Writing bronze kline data to base path: {BRONZE_BASE_PATH}", flush=True)

    (
        df.repartition("symbol", "date", "hour")
        .write
        .format("parquet")
        .mode("overwrite")
        .partitionBy("symbol", "date", "hour")
        .save(BRONZE_BASE_PATH)
    )


def main() -> None:
    start_time, end_time = parse_hour_range()
    print(f"Processing range: start={start_time}, end={end_time}", flush=True)

    spark = get_spark_session()

    try:
        raw_paths = build_existing_raw_paths(
            spark=spark,
            start_time=start_time,
            end_time=end_time,
        )
        raw_df = read_raw_klines(spark=spark, raw_paths=raw_paths)
        bronze_df = transform_to_bronze(raw_df)
        write_bronze_klines(bronze_df)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()