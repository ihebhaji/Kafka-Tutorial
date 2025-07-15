# Consumers.py
from abc import ABC, abstractmethod
from pydantic import BaseModel, Field, validator
from confluent_kafka import Consumer, KafkaError
import os
import pandas as pd
import time
import json
import fastavro
import struct
import io
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import SerializationContext, MessageField


def _make_consumer(config: dict) -> Consumer:
    try:
        c = Consumer(config)
    except Exception as e:
        raise RuntimeError(f"Failed to create Kafka Consumer: {e}")
    return c


class ConsumerBase(BaseModel, ABC):
    topic: str = Field(..., description="Kafka topic name")
    group_id: str = Field(
        "cloud-python-group", description="Consumer group ID"
    )
    bootstrap_servers: str = Field(
        ..., description="Kafka bootstrap servers URL(s)"
    )
    security_protocol: str = Field("SASL_SSL")
    sasl_mechanisms: str = Field("PLAIN")
    sasl_username: str
    sasl_password: str
    auto_offset_reset: str = Field("earliest")
    output_dir: str = Field("output")
    batch_size: int = Field(5, ge=1)
    timeout_sec: float = Field(30.0, ge=1.0)

    class Config:
        env_prefix = "CONFLUENT_"
        arbitrary_types_allowed = True

    @validator("bootstrap_servers", pre=True)
    def must_not_be_empty(cls, v):
        if not v:
            raise ValueError("bootstrap_servers must be provided")
        return v

    def _consumer_config(self) -> dict:
        return {
            "bootstrap.servers": self.bootstrap_servers,
            "security.protocol": self.security_protocol,
            "sasl.mechanisms": self.sasl_mechanisms,
            "sasl.username": self.sasl_username,
            "sasl.password": self.sasl_password,
            "group.id": self.group_id,
            "auto.offset.reset": self.auto_offset_reset,
        }

    @abstractmethod
    def _deserialize(self, raw_bytes: bytes) -> dict: ...

    def consume(self) -> None:
        os.makedirs(self.output_dir, exist_ok=True)
        consumer = _make_consumer(self._consumer_config())
        consumer.subscribe([self.topic])
        records = []
        start = time.time()
        try:
            while True:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    err = msg.error()
                    if not err.code() == KafkaError._PARTITION_EOF:
                        print(f"Error: {err}")
                    continue
                try:
                    record = self._deserialize(msg.value())
                except Exception as e:
                    print(f"Deserialization error: {e}")
                    continue
                records.append(record)
                print(f"Consumed: {record}")
                if len(records) >= self.batch_size:
                    df = pd.DataFrame(records)
                    path = (
                        f"{self.output_dir}/{self.topic}_{time.time()}.parquet"
                    )
                    df.to_parquet(path, index=False)
                    print(f"Saved batch to {path}")
                    records = []
                if time.time() - start > self.timeout_sec:
                    break
        finally:
            if records:
                df = pd.DataFrame(records)
                path = f"{self.output_dir}/{self.topic}_{time.time()}.parquet"
                df.to_parquet(path, index=False)
                print(f"Saved final batch to {path}")
            consumer.close()


class ConsumerJson(ConsumerBase):
    def _deserialize(self, raw_bytes: bytes) -> dict:
        return json.loads(raw_bytes.decode("utf-8"))


class ConsumerEmbedded(ConsumerBase):
    def _deserialize(self, raw_bytes: bytes) -> dict:
        # embedded schema Avro
        schema_length = struct.unpack(">I", raw_bytes[:4])[0]
        schema_start = 4
        schema_end = schema_start + schema_length
        schema_json = raw_bytes[schema_start:schema_end].decode("utf-8")
        schema_dict = json.loads(schema_json)
        parsed = fastavro.parse_schema(schema_dict)
        avro_data = raw_bytes[schema_end:]
        buf = io.BytesIO(avro_data)
        if avro_data.startswith(b"Obj\x01"):
            reader = fastavro.reader(buf)
            recs = list(reader)
            return recs[0] if recs else {}
        else:
            return fastavro.schemaless_reader(buf, parsed)


class ConsumerSchemaReg(ConsumerBase):
    schema_registry_url: str
    schema_registry_api_key: str
    schema_registry_api_secret: str

    def _get_deserializer(self) -> AvroDeserializer:
        conf = {
            "url": self.schema_registry_url,
            "basic.auth.user.info": f"{self.schema_registry_api_key}:{self.schema_registry_api_secret}",  # type: ignore
        }
        client = SchemaRegistryClient(conf)
        subject = f"{self.topic}-value"
        latest = client.get_latest_version(subject)
        schema_str = latest.schema.schema_str
        return AvroDeserializer(client, schema_str, lambda obj, ctx: obj)

    def _deserialize(self, raw_bytes: bytes) -> dict:
        if not hasattr(self, "_deserializer"):
            object.__setattr__(self, "_deserializer", self._get_deserializer())
        ctx = SerializationContext(self.topic, MessageField.VALUE)
        return self._deserializer(raw_bytes, ctx)


if __name__ == "__main__":
    pass
