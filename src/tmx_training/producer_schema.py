# Import the Kafka Producer class
from confluent_kafka import Producer
# Import Avro serializer
from confluent_kafka.schema_registry.avro import AvroSerializer
# Import Schema Registry client
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import (
    SerializationContext,
    MessageField
)  # Import serialization context
import socket
import os



print("🚀 Starting Kafka Producer...")
# Define the topic to produce messages to
TOPIC = 'tmx-training' 
# Define the subject name for schema registration
subject_name = f"{TOPIC}-value"
print(f"📤 Producing messages to topic: {TOPIC} with subject: {subject_name}")
# load the avro schema registry from confluent cloud
schema_registry_conf = {
    'url': os.getenv("CONFLUENT_SCHEMA_REGISTRY_URL"),
    'basic.auth.user.info': (
        f"{os.getenv('CONFLUENT_SCHEMA_REGISTRY_KEY')}:"
        f"{os.getenv('CONFLUENT_SCHEMA_REGISTRY_SECRET')}"
    )
}
print(f"🔗 Connecting to Schema Registry at {schema_registry_conf['url']}...")
schema_registry_client = SchemaRegistryClient(schema_registry_conf)
latest_schema = schema_registry_client.get_latest_version(subject_name)
avro_schema = latest_schema.schema  # Get the Avro schema from the registry
avro_serializer = AvroSerializer(
    schema_registry_client,
    avro_schema.schema_str,  # Use the schema string from the registry
    lambda obj, ctx:  obj # Serialize the object to Avro binary format
)

print("⚙️ Configuring Kafka Producer...")
conf = {'bootstrap.servers': os.getenv("CONFLUENT_BOOTSTRAPP"),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': os.getenv("CONFLUENT_API_KEY"),
    'sasl.password': os.getenv("CONFLUENT_API_SECRET"),
    'client.id': socket.gethostname()}

print(f"🔌 Connecting to Kafka cluster at {conf['bootstrap.servers']}...")
producer = Producer(conf)        # Instantiate Producer

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

if __name__ == '__main__':
    # 3. Send 5 messages
    for i in range(5):
        
        record_value = {"id": i,
                       "name": f"User {i}",
                       "email": f"user{i}@example.com"}
        
        # Serialize the record using Avro schema
        serialized_value = avro_serializer(
                                record_value,
                                SerializationContext(TOPIC, MessageField.VALUE)
                            )

        # 4. Produce message asynchronously
        producer.produce(
            topic=TOPIC,    # Target topic
            key=str(i),            # Optional key
            value=serialized_value,        # Serialized value
            callback=delivery_report  # Called when message sent
        )
        producer.poll(0)         # Trigger delivery callbacks immediately

    # 5. Wait for outstanding messages to be delivered
    producer.flush()