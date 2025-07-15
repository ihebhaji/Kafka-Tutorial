# Import the Kafka Producer class
from confluent_kafka import Producer
import json
import socket
import os
import fastavro
import io
import struct
from datetime import datetime


def serialize_embedded(schema: dict, data: dict) -> bytes:
    """Serialize data with embedded schema using fastavro.writer"""
    # 1. Create schema bytes
    schema_bytes = json.dumps(schema).encode("utf-8")

    # 2. Use fastavro.writer (creates complete Avro file)
    avro_writer = io.BytesIO()
    fastavro.writer(avro_writer, schema, [data])
    avro_bytes = avro_writer.getvalue()

    # 3. Create payload: [4-byte schema length][schema][avro file data]
    schema_length = struct.pack(">I", len(schema_bytes))
    payload = schema_length + schema_bytes + avro_bytes
    return payload


print("🚀 Starting Kafka Producer...")
# Define the topic to produce messages to
TOPIC = "tmx-training"
# Define the subject name for schema registration
subject_name = f"{TOPIC}-value"
print(f"📤 Producing messages to topic: {TOPIC} with subject: {subject_name}")

SCHEMA = {
    "type": "record",
    "name": "User",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "name", "type": "string"},
        {"name": "email", "type": "string"},
        {
            "name": "signup_ts",
            "type": {"type": "long", "logicalType": "timestamp-millis"},
        },
    ],
}  # Define the Avro schema for the messages

print("⚙️ Configuring Kafka Producer...")
conf = {
    "bootstrap.servers": os.getenv("CONFLUENT_BOOTSTRAPP"),
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "PLAIN",
    "sasl.username": os.getenv("CONFLUENT_API_KEY"),
    "sasl.password": os.getenv("CONFLUENT_API_SECRET"),
    "client.id": socket.gethostname(),
}

print(f"🔌 Connecting to Kafka cluster at {conf['bootstrap.servers']}...")
producer = Producer(conf)  # Instantiate Producer


# 2. Callback to report delivery success/failure
def delivery_report(err, msg):
    if err:
        # Report the error if message delivery failed
        print(f"❌ Delivery failed: {err}")
    else:
        # Report successful message delivery
        print(
            f"✅ Message delivered to {msg.topic()} [{msg.partition()}] "
            f"at offset {msg.offset()} with key {msg.key().decode('utf-8')}"
        )


if __name__ == "__main__":
    # 3. Send 5 messages
    for i in range(5):

        record_value = {
            "id": i,
            "name": f"User {i}",
            "email": f"user{i}@example.com",
            "signup_ts": int(datetime.now().timestamp() * 1000),
        }
        payload = serialize_embedded(
            SCHEMA, record_value
        )  # Serialize the record

        # 4. Produce message asynchronously
        producer.produce(
            topic=TOPIC,  # Target topic
            key=str(i),  # Optional key
            value=payload,  # Serialized value
            headers={
                "encoding": "embedded_avro"
            },  # Custom header to indicate embedded Avro
            callback=delivery_report,  # Callback for delivery report
        )
        producer.poll(0)  # Trigger delivery callbacks immediately

    # 5. Wait for outstanding messages to be delivered
    producer.flush()
