import json

from app.utils.schema_registry import USERS_COORDINATES_JSON_SCHEMA, get_schema_registry_url
from app.utils.kafka import get_bootstrap_server, get_users_coordinates_topic
from app.utils.generator import generate_user_coordinates

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import StringSerializer

def delivery_report(err, msg):
    if err:
        print("Delivery failed:", err)
    else:
        print(f"Delivered to {msg.topic()} [{msg.partition()}]")


def main():
    # Получаем схему, брокер и топик
    schema_registry_url = get_schema_registry_url()
    bootstrap_server = get_bootstrap_server()
    users_coordinates_topic = get_users_coordinates_topic()

    schema_registry_client = SchemaRegistryClient({"url": schema_registry_url})

    json_schema_str = json.dumps(USERS_COORDINATES_JSON_SCHEMA)

    json_serializer = JSONSerializer(
        json_schema_str,
        schema_registry_client
    )

    producer_conf = {
        "bootstrap.servers": bootstrap_server,
        "key.serializer": StringSerializer("utf_8"),
        "value.serializer": json_serializer,
    }

    producer = SerializingProducer(producer_conf)

    data = generate_user_coordinates()

    producer.produce(
        topic=users_coordinates_topic,
        key=str(data["id"]),
        value=data,
        on_delivery=delivery_report,
    )

    producer.flush()

if __name__ == "__main__":
    main()
