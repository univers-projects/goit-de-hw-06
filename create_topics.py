"""Create the input and output topics for the Spark streaming homework."""

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from config import ALERT_TOPIC, BOOTSTRAP_SERVERS, SENSOR_TOPIC


def main() -> None:
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
    topics = (SENSOR_TOPIC, ALERT_TOPIC)
    try:
        try:
            admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1) for topic in topics])
            print("Topics created:")
        except TopicAlreadyExistsError:
            print("Topics already exist:")

        available_topics = set(admin.list_topics())
        for topic in topics:
            print(f"  {'OK' if topic in available_topics else 'MISSING'}  {topic}")
    finally:
        admin.close()


if __name__ == "__main__":
    main()
