# Import the Kafka Producer class
from confluent_kafka import Producer
import json
import socket
import os



print("🚀 Starting Kafka Producer...")
# Define the topic to produce messages to
TOPIC = 'tmx-training' 
# Define the subject name for schema registration
subject_name = f"{TOPIC}-value"
print(f"📤 Producing messages to topic: {TOPIC} with subject: {subject_name}")
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
        serialized_value = json.dumps(record_value).encode('utf-8')

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