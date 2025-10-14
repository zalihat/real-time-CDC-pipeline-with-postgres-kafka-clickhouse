from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
import json

# --- Config ---
conf = {
    'bootstrap.servers': 'localhost:29092',
    'group.id': 'cdc-consumer-group',
    'auto.offset.reset': 'earliest'
}

schema_registry_conf = {'url': 'http://localhost:8081'}  # host access to schema registry
schema_registry_client = SchemaRegistryClient(schema_registry_conf)
avro_deserializer = AvroDeserializer(schema_registry_client)

# --- Create Consumer ---
consumer = Consumer(conf)
consumer.subscribe(['debezium.public.sales_test'])

print("Consuming messages. Press Ctrl+C to exit.")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Error:", msg.error())
            continue

        value = msg.value()
        if value is None:
            continue

        # Deserialize Avro data
        record = avro_deserializer(value, None)

        # Pretty print JSON output
        print(json.dumps(record, indent=2))

except KeyboardInterrupt:
    print("Exiting consumer...")
finally:
    consumer.close()
