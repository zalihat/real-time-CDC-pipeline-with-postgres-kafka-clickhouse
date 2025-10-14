from confluent_kafka import Consumer, KafkaException
import json

# Kafka configuration
conf = {
    'bootstrap.servers': 'localhost:29092',  # adjust if using docker or remote broker
    'group.id': 'python-consumer-group',
    'auto.offset.reset': 'earliest'
}

# Create consumer instance
consumer = Consumer(conf)
topic = "debezium.public.sales_test"  # your topic name

consumer.subscribe([topic])

print(f"Listening for messages on topic '{topic}'...")

try:
    while True:
        msg = consumer.poll(1.0)  # timeout 1s
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())

        try:
            data = json.loads(msg.value().decode('utf-8'))

            # Extract 'after' payload if it exists
            payload = data.get("payload", {})
            after = payload.get("after")

            print("\n--- New Event ---")
            print(json.dumps(after, indent=4))
        except json.JSONDecodeError:
            print("Received non-JSON message:", msg.value())

except KeyboardInterrupt:
    print("Stopping consumer...")

finally:
    consumer.close()
