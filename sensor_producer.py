"""Generate one independent temperature and humidity sensor stream."""

import json
import random
import time
from argparse import ArgumentParser
from datetime import datetime, timezone

from kafka import KafkaProducer

from config import BOOTSTRAP_SERVERS, SENSOR_TOPIC


PROFILES = {
    "normal": ((25, 45), (15, 85)),
    "hot": ((41, 45), (35, 75)),
    "wet": ((30, 38), (81, 85)),
    "dry": ((30, 38), (15, 19)),
}


def parse_args():
    parser = ArgumentParser(description="Generate one IoT sensor stream.")
    parser.add_argument(
        "--profile",
        choices=PROFILES,
        default="normal",
        help="Sensor value range; use hot/wet/dry to demonstrate an alert.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    sensor_id = f"sensor-{random.randint(1000, 9999)}"
    temperature_range, humidity_range = PROFILES[args.profile]
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )
    print(f"Started {sensor_id} ({args.profile}); writing to {SENSOR_TOPIC}. Press Ctrl+C to stop.")
    try:
        while True:
            reading = {
                "id": sensor_id,
                "temperature": round(random.uniform(*temperature_range), 2),
                "humidity": round(random.uniform(*humidity_range), 2),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            metadata = producer.send(SENSOR_TOPIC, reading).get(timeout=10)
            print(f"Sent: {reading} (partition={metadata.partition}, offset={metadata.offset})")
            time.sleep(2)
    except KeyboardInterrupt:
        print("\nSensor producer stopped.")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
