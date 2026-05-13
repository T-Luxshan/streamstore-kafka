import json
import uuid

from confluent_kafka import Producer

producer_config = {
    "bootstrap.servers": 'localhost:9092'
}


def delivery_report(err, msg):
    if err:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered {msg.value().decode('utf-8')}")


producer = Producer(producer_config)

order = {
    "order_id": str(uuid.uuid4()),
    "user": "Luxshan",
    "item": "Pizza",
    "quantity": 2
}

value = json.dumps(order).encode("utf-8")

for i in range(5):
    producer.produce(
        topic="orders",
        value=value,
        callback=delivery_report
    )

producer.flush()
