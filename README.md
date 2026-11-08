# IoT sensor alerts with Spark Structured Streaming

This project reads sensor events from Kafka, calculates sliding-window averages, compares them with alert rules from a CSV file, and writes matching alerts to Kafka.

## Flow

```text
sensor_producer.py -> building_sensors_spark_univers
                         |
                         v
                 spark_alerts.py
        (1 min window / 30 sec slide / 10 sec watermark)
                         |
                         v
                  sensor_alerts_spark_univers
```

`alerts_conditions.csv` is the rule source. A value of `-999` means that its limit is disabled for the rule.

## Requirements

- Python 3.11+ and PySpark 4.2.0;
- a Kafka broker at `localhost:9092` (the Docker Compose setup from homework 05 can be reused);
- internet access on the first Spark run, so Spark can download `spark-sql-kafka-0-10_2.13:4.2.0`.

The included `.env.example` documents optional local settings. Defaults require no configuration.

To override defaults, copy it first:

```bash
cp .env.example .env
```

## Run

```bash
cd goit-de-hw-06
source ../goit_spark-sandbox/.venv/bin/activate
python create_topics.py
```

Open two terminals in the same environment:

```bash
# terminal 1: Spark Structured Streaming job
python spark_alerts.py

# terminal 2: a sensor stream (run more copies for multiple sensors)
python sensor_producer.py
```

The Spark job emits a completed window after the watermark advances. Keep the producer running for at least 90 seconds to observe alerts. For a reliable alert demonstration, start a controlled sensor profile in a second terminal:

```bash
python sensor_producer.py --profile hot
# Other options: --profile wet, --profile dry
```

## Inspect the result

In Kafka UI (`http://localhost:8080` from homework 05), open `sensor_alerts_spark_univers` → **Messages**. Alert messages contain the window start/end, `t_avg`, `h_avg`, alert `code`, `message`, and creation `timestamp`.

## Main implementation details

- `sensor_producer.py` produces JSON records with `id`, `temperature`, `humidity`, and `timestamp`.
- `spark_alerts.py` uses `withWatermark("event_time", "10 seconds")` and `window("event_time", "1 minute", "30 seconds")`.
- The aggregated stream uses a broadcast cross join with the static CSV rules and filters rows that exceed an enabled boundary.
- Matching rows are serialized as JSON and written to the alert Kafka topic.
