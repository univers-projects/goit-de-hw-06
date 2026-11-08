"""Configuration shared by the producer, topic creator, and Spark job."""

import os
import re


def _load_dotenv_if_available() -> None:
    """Load a local .env file when python-dotenv is installed."""
    try:
        from dotenv import load_dotenv

        load_dotenv()
    except ImportError:
        pass


_load_dotenv_if_available()

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_SUFFIX = re.sub(r"[^A-Za-z0-9._-]", "_", os.getenv("TOPIC_SUFFIX", "univers"))
SENSOR_TOPIC = f"building_sensors_spark_{TOPIC_SUFFIX}"
ALERT_TOPIC = f"sensor_alerts_spark_{TOPIC_SUFFIX}"

# The version must match the installed PySpark distribution. Override when needed.
SPARK_KAFKA_PACKAGE = os.getenv(
    "SPARK_KAFKA_PACKAGE", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0"
)
