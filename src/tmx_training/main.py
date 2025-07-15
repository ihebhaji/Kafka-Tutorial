import os
from consumers import (
    ConsumerJson,
    ConsumerEmbedded,
    ConsumerSchemaReg
)
from producers import (
    ProducerJson,
    ProducerEmbedded,
    ProducerSchemaReg
)


def main():
    # initialize producers
    json_producer = ProducerJson(
        topic="tmx-training",
        bootstrap_servers=os.getenv("CONFLUENT_BOOTSTRAPP"),
        sasl_username=os.getenv("CONFLUENT_API_KEY"),
        sasl_password=os.getenv("CONFLUENT_API_SECRET"),
    )
    embedded_producer = ProducerEmbedded(
        topic="tmx-training",
        bootstrap_servers=os.getenv("CONFLUENT_BOOTSTRAPP"),
        sasl_username=os.getenv("CONFLUENT_API_KEY"),
        sasl_password=os.getenv("CONFLUENT_API_SECRET"),
        schema={
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"},
                {"name": "email", "type": "string"},
            ],
        },
    )
    schema_producer = ProducerSchemaReg(
        topic="tmx-training",
        bootstrap_servers=os.getenv("CONFLUENT_BOOTSTRAPP"),
        sasl_username=os.getenv("CONFLUENT_API_KEY"),
        sasl_password=os.getenv("CONFLUENT_API_SECRET"),
        schema_registry_url=os.getenv("CONFLUENT_SCHEMA_REGISTRY_URL"),
        schema_registry_api_key=os.getenv("CONFLUENT_SCHEMA_REGISTRY_KEY"),
        schema_registry_api_secret=os.getenv(
            "CONFLUENT_SCHEMA_REGISTRY_SECRET"
        ),
    )

    # send sample
    for i in range(3):
        rec = {"id": i, "name": f"User {i}", "email": f"user{i}@example.com"}
        json_producer.produce(rec, key=str(i))
        embedded_producer.produce(rec, key=str(i))
        schema_producer.produce(rec, key=str(i))

    # initialize consumers
    json_consumer = ConsumerJson(
        topic="tmx-training",
        bootstrap_servers=os.getenv("CONFLUENT_BOOTSTRAPP"),
        sasl_username=os.getenv("CONFLUENT_API_KEY"),
        sasl_password=os.getenv("CONFLUENT_API_SECRET"),
    )
    embedded_consumer = ConsumerEmbedded(
        topic="tmx-training",
        bootstrap_servers=os.getenv("CONFLUENT_BOOTSTRAPP"),
        sasl_username=os.getenv("CONFLUENT_API_KEY"),
        sasl_password=os.getenv("CONFLUENT_API_SECRET"),
    )
    schema_consumer = ConsumerSchemaReg(
        topic="tmx-training",
        bootstrap_servers=os.getenv("CONFLUENT_BOOTSTRAPP"),
        sasl_username=os.getenv("CONFLUENT_API_KEY"),
        sasl_password=os.getenv("CONFLUENT_API_SECRET"),
        schema_registry_url=os.getenv("CONFLUENT_SCHEMA_REGISTRY_URL"),
        schema_registry_api_key=os.getenv("CONFLUENT_SCHEMA_REGISTRY_KEY"),
        schema_registry_api_secret=os.getenv(
            "CONFLUENT_SCHEMA_REGISTRY_SECRET"
        ),
    )

    # run consumers
    print("\nJSON Consumer:")
    json_consumer.consume()
    print("\nEmbedded Consumer:")
    embedded_consumer.consume()
    print("\nSchema Registry Consumer:")
    schema_consumer.consume()


if __name__ == "__main__":
    main()
