"""Aggregate sensor events in sliding windows and write matching alerts to Kafka."""

import os
from pathlib import Path

from config import ALERT_TOPIC, BOOTSTRAP_SERVERS, SENSOR_TOPIC, SPARK_KAFKA_PACKAGE

# This makes `python spark_alerts.py` fetch the Kafka connector if Spark does not
# already have it. It must be set before importing pyspark.
os.environ.setdefault("PYSPARK_SUBMIT_ARGS", f"--packages {SPARK_KAFKA_PACKAGE} pyspark-shell")

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    broadcast,
    col,
    current_timestamp,
    from_json,
    lit,
    struct,
    to_json,
    to_timestamp,
    when,
    window,
)
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

BASE_DIR = Path(__file__).resolve().parent
CHECKPOINT_DIR = BASE_DIR / "checkpoints" / "sensor_alerts"

SENSOR_SCHEMA = StructType(
    [
        StructField("id", StringType(), nullable=False),
        StructField("temperature", DoubleType(), nullable=False),
        StructField("humidity", DoubleType(), nullable=False),
        StructField("timestamp", StringType(), nullable=False),
    ]
)


def condition_matches():
    """Build the boolean expression that handles -999 as an unused boundary."""
    return (
        ((col("min_temperature") != -999) & (col("t_avg") < col("min_temperature")))
        | ((col("max_temperature") != -999) & (col("t_avg") > col("max_temperature")))
        | ((col("min_humidity") != -999) & (col("h_avg") < col("min_humidity")))
        | ((col("max_humidity") != -999) & (col("h_avg") > col("max_humidity")))
    )


def main() -> None:
    spark = (
        SparkSession.builder.appName("IoT-Sensor-Alerts")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    conditions = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(str(BASE_DIR / "alerts_conditions.csv"))
        .cache()
    )

    kafka_events = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", SENSOR_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    readings = (
        kafka_events.select(from_json(col("value").cast("string"), SENSOR_SCHEMA).alias("reading"))
        .select("reading.*")
        .withColumn("event_time", to_timestamp("timestamp"))
        .filter(col("event_time").isNotNull())
    )

    averages = (
        readings.withWatermark("event_time", "10 seconds")
        .groupBy(window("event_time", "1 minute", "30 seconds"))
        .agg(avg("temperature").alias("t_avg"), avg("humidity").alias("h_avg"))
    )

    alerts = averages.crossJoin(broadcast(conditions)).where(condition_matches())
    alert_messages = alerts.select(
        lit(None).cast("string").alias("key"),
        to_json(
            struct(
                col("window"),
                col("t_avg"),
                col("h_avg"),
                col("code").cast("string").alias("code"),
                col("message"),
                current_timestamp().alias("timestamp"),
            )
        ).alias("value"),
    )

    print(f"Reading {SENSOR_TOPIC}; writing alerts to {ALERT_TOPIC}.")
    print("Window: 1 minute; slide: 30 seconds; watermark: 10 seconds. Press Ctrl+C to stop.")
    query = (
        alert_messages.writeStream.format("kafka")
        .outputMode("append")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("topic", ALERT_TOPIC)
        .option("checkpointLocation", str(CHECKPOINT_DIR))
        .trigger(processingTime="10 seconds")
        .start()
    )
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        query.stop()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
