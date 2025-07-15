from confluent_kafka import Consumer
import os
import pandas as pd
import time
import json

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
    "sasl.mechanisms": "PLAIN",
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
                user_record = json.loads(msg.value().decode("utf-8"))
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
