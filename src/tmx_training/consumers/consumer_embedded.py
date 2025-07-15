from confluent_kafka import Consumer
import os
import pandas as pd
import time
import fastavro
import struct
import json
import io

print("Starting Kafka Consumer...")
# Define the topic to produce messages to
TOPIC = "tmx-training"
# Define the subject name for schema registration
subject_name = f"{TOPIC}-value"
print(f"Consuming messages from topic: {TOPIC} with subject: {subject_name}")

print("Configuring Kafka Consumer...")
# Configure the consumer
conf = {
    "bootstrap.servers": os.getenv("CONFLUENT_BOOTSTRAPP"),
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "PLAIN",  # Fixed: was 'sasl.mechanisms'
    "sasl.username": os.getenv("CONFLUENT_API_KEY"),
    "sasl.password": os.getenv("CONFLUENT_API_SECRET"),
    "group.id": "cloud-python-group",
    "auto.offset.reset": "earliest",
}

print(f"Connecting to Kafka cluster at {conf['bootstrap.servers']}...")
# Instantiate Consumer
print("Creating Kafka Consumer...")
consumer = Consumer(conf)
consumer.subscribe(["tmx-training"])


def deserialize_embedded(payload: bytes) -> dict:
    """Deserialize payload with embedded schema - handles both formats"""
    try:
        # 1. Extract schema length (first 4 bytes)
        schema_length = struct.unpack(">I", payload[:4])[0]

        # 2. Extract schema JSON
        schema_start = 4
        schema_end = schema_start + schema_length
        schema_json = payload[schema_start:schema_end].decode("utf-8")

        # 3. Parse schema (critical for logical types)
        schema_dict = json.loads(schema_json)
        parsed_schema = fastavro.parse_schema(schema_dict)

        # 4. Extract Avro data
        avro_data = payload[schema_end:]

        # 5. Check if it's a complete Avro file or raw schemaless data
        if avro_data.startswith(b"Obj\x01"):
            # This is a complete Avro file (from fastavro.writer)
            print("🔍 Detected complete Avro file format")
            buf = io.BytesIO(avro_data)
            reader = fastavro.reader(buf)
            records = list(reader)
            if records:
                return records[0]  # Return the first record
            else:
                raise ValueError("No records found in Avro file")
        else:
            # This is raw schemaless data (from fastavro.schemaless_writer)
            print("🔍 Detected schemaless Avro format")
            buf = io.BytesIO(avro_data)
            return fastavro.schemaless_reader(buf, parsed_schema)

    except Exception as e:
        print(f"🚨 Deserialization error: {str(e)}")
        print(f"🔍 Payload length: {len(payload)}")
        print(
            f"🔍 Schema length: {schema_length if 'schema_length' in locals() else 'N/A'}"
        )
        print(f"🔍 First 50 bytes of payload: {payload[:50]}")
        if "avro_data" in locals():
            print(f"🔍 First 20 bytes of avro_data: {avro_data[:20]}")
        raise


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
                user_record = deserialize_embedded(msg.value())
                print(f"📥 Consumed message: {user_record}")
                records.append(user_record)

                # Save to parquet after collecting some messages
                if len(records) >= 5:  # Adjust batch size as needed
                    df = pd.DataFrame(records)
                    df.to_parquet(
                        f"output/users_{time.time()}.parquet", index=False
                    )
                    print(f"✅ Saved {len(records)} messages to parquet")
                    records = []
                    df.to_csv(f"output/users_{time.time()}.csv", index=False)

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
            df.to_parquet(f"output/users_{time.time()}.parquet", index=False)
            print(f"Saved remaining {len(records)} messages to parquet")
        consumer.close()
        print("🚀 Consumer finished successfully.")
        print("📦 All messages processed and saved to parquet files.")
        print("📁 Output files saved in 'output' directory.")


if __name__ == "__main__":
    os.makedirs("output", exist_ok=True)  # Ensure output directory exists
    main()
