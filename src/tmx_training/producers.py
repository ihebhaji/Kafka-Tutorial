from abc import ABC, abstractmethod
from pydantic import BaseModel, Field, validator
from confluent_kafka import Producer
import json
import fastavro
import io
import struct
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import SerializationContext, MessageField
import socket


def _make_producer(config: dict) -> Producer:
    try:
        p = Producer(config)
    except Exception as e:
        raise RuntimeError(f"Failed to create Kafka Producer: {e}")
    return p


class ProducerBase(BaseModel, ABC):
    topic: str = Field(...)
    bootstrap_servers: str = Field(...)
    security_protocol: str = Field("SASL_SSL")
    sasl_mechanisms: str = Field("PLAIN")
    sasl_username: str
    sasl_password: str
    client_id: str = Field(default_factory=socket.gethostname)

    class Config:
        env_prefix = "CONFLUENT_"
        arbitrary_types_allowed = True

    def _producer_config(self) -> dict:
        conf = {
            "bootstrap.servers": self.bootstrap_servers,
            "security.protocol": self.security_protocol,
            "sasl.mechanisms": self.sasl_mechanisms,
            "sasl.username": self.sasl_username,
            "sasl.password": self.sasl_password,
            "client.id": self.client_id,
        }
        return conf

    @abstractmethod
    def _serialize(self, record: dict) -> bytes: ...

    def produce(self, record: dict, key: str = None) -> None:
        producer = _make_producer(self._producer_config())
        payload = self._serialize(record)

        def delivery_report(err, msg):
            if err:
                print(f"Delivery failed: {err}")
            else:
                print(
                    f"Produced to {msg.topic()} "
                    f"[{msg.partition()}] @ {msg.offset()}"
                )

        producer.produce(
            topic=self.topic, value=payload, key=key, callback=delivery_report
        )
        producer.flush()


class ProducerJson(ProducerBase):
    def _serialize(self, record: dict) -> bytes:
        return json.dumps(record).encode("utf-8")


class ProducerEmbedded(ProducerBase):
    schema: dict

    @validator("schema")
    def schema_must_have_fields(cls, s):
        if not isinstance(s, dict):
            raise ValueError("schema must be a dict")
        return s

    def _serialize(self, record: dict) -> bytes:
        schema_bytes = json.dumps(self.schema).encode("utf-8")
        buf = io.BytesIO()
        fastavro.writer(buf, self.schema, [record])
        avro_bytes = buf.getvalue()
        header = struct.pack(">I", len(schema_bytes))
        return header + schema_bytes + avro_bytes


class ProducerSchemaReg(ProducerBase):
    schema_registry_url: str
    schema_registry_api_key: str
    schema_registry_api_secret: str

    def _get_serializer(self) -> AvroSerializer:
        conf = {
            "url": self.schema_registry_url,
            "basic.auth.user.info": (
                f"{self.schema_registry_api_key}:{self.schema_registry_api_secret}"
            ),
        }
        client = SchemaRegistryClient(conf)
        subject = f"{self.topic}-value"
        latest = client.get_latest_version(subject)
        schema_str = latest.schema.schema_str
        return AvroSerializer(client, schema_str, lambda obj, ctx: obj)

    def _serialize(self, record: dict) -> bytes:
        if not hasattr(self, "_serializer"):
            object.__setattr__(self, "_serializer", self._get_serializer())
        ctx = SerializationContext(self.topic, MessageField.VALUE)
        return self._serializer(record, ctx)
