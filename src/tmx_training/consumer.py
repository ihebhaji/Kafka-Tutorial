from confluent_kafka import Consumer
import os
import pandas as pd
import time
# Import AvroDeserializer for deserializing Avro messages
from confluent_kafka.schema_registry.avro import AvroDeserializer
 # Import Schema Registry client
from confluent_kafka.schema_registry import (
    SchemaRegistryClient
)
# Import necessary classes for serialization context and message fields
from confluent_kafka.serialization import (
    SerializationContext,
    MessageField
)  



print("Starting Kafka Consumer...")
# Define the topic to produce messages to
TOPIC = 'tmx-training'
# Define the subject name for schema registration
subject_name = f"{TOPIC}-value"
print(f"Consuming messages from topic: {TOPIC} with subject: {subject_name}")
# load the avro schema registry from confluent cloud
schema_registry_conf = {
    'url': os.getenv("CONFLUENT_SCHEMA_REGISTRY_URL"),
    'basic.auth.user.info': (
        f"{os.getenv('CONFLUENT_SCHEMA_REGISTRY_KEY')}:"
        f"{os.getenv('CONFLUENT_SCHEMA_REGISTRY_SECRET')}"
    )
}
print(f"Connecting to Schema Registry at {schema_registry_conf['url']}...")
schema_registry_client = SchemaRegistryClient(schema_registry_conf)
latest_schema = schema_registry_client.get_latest_version(subject_name)
avro_schema = latest_schema.schema  # Get the Avro schema from the registry
avro_deserializer = AvroDeserializer(
    schema_registry_client,
    avro_schema.schema_str,  # Use the schema string from the registry
    lambda obj, ctx:  obj # Deserialize the object from Avro binary format
)

print("Configuring Kafka Consumer...")

# Configure the consumer
conf = {
    'bootstrap.servers': os.getenv("CONFLUENT_BOOTSTRAPP"),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.getenv("CONFLUENT_API_KEY"),
    'sasl.password': os.getenv("CONFLUENT_API_SECRET"),
    'group.id': 'cloud-python-group',
    'auto.offset.reset': 'earliest'
}

print(f"Connecting to Kafka cluster at {conf['bootstrap.servers']}...")
# Instantiate Consumer
print("Creating Kafka Consumer...")

consumer = Consumer(conf)
consumer.subscribe(['tmx-training'])

def main() -> None:
    
    print("📡 Listening for messages (Ctrl+C to exit)...")
    # Process messages
    records = []
    init_time = time.time()
    try:
        while True:
            msg = consumer.poll(timeout=1.0)  # Wait 1 seconds
            if msg is None:
                print("⏳ No message received, waiting...")
                continue
            if msg.error():
                print(f"❌ Error: {msg.error()}")
            else:
                # Deserialize message
                user_record = avro_deserializer(
                    msg.value(),
                    SerializationContext(TOPIC, MessageField.VALUE)
                )
                print(f"📥 Consumed message: {user_record}")
                records.append(user_record)
                # Save to parquet after collecting some messages
                if len(records) >= 5:  # Adjust batch size as needed
                    df = pd.DataFrame(records)
                    df.to_parquet(f'output/users_{time.time()}.parquet',
                                  index=False)
                    print(f"✅ Saved {len(records)} messages to parquet")
                    records = []
            
            # Check if 30 seconds have passed
            if time.time() - init_time > 30:  # Check if 30 seconds have passed
                    print("⏰ 30 seconds elapsed, stopping consumer...")
                    break   
    except KeyboardInterrupt:
        print("🛑 Consumer stopped")
    finally:
        if records:
            # Save any remaining records to parquet
            print(f"📊 Consumed {len(records)} records")
            df = pd.DataFrame(records)
            df.to_parquet(f'output/users_{time.time()}.parquet', index=False)
            print(f"Saved remaining {len(records)} messages to parquet")
        consumer.close()

        print("🚀 Consumer finished successfully.")
        print("📦 All messages processed and saved to parquet files.")
        print("📁 Output files saved in 'output' directory.")
        
if __name__ == '__main__':
    os.makedirs('output', exist_ok=True)  # Ensure output directory exists
    main()
