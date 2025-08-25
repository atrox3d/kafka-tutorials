import json
from kafka import KafkaProducer, KafkaConsumer
from typing import Callable, Any

from config import (
    KAFKA_BROKER_URL
)


def json_serializer(data: Any) -> bytes:
    """Default JSON serializer."""
    
    return json.dumps(data).encode("utf-8")


def json_deserializer(data: bytes) -> Any:
    """Default JSON deserializer."""
    
    if not data:
        return None
    return json.loads(data.decode("utf-8"))


def create_producer(
    bootstrap_servers: str | list[str] = KAFKA_BROKER_URL,
    value_serializer: Callable[[Any], bytes] = json_serializer,
) -> KafkaProducer:
    """Creates a KafkaProducer with sensible defaults."""
    
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=value_serializer
    )
    return producer


def create_consumer(
    *topics: str,
    bootstrap_servers: str | list[str] = KAFKA_BROKER_URL,
    group_id: str = None,
    auto_offset_reset: str = 'earliest',
    enable_auto_commit: bool = False,
    value_deserializer: Callable[[bytes], Any] = json_deserializer,
) -> KafkaConsumer:
    """Creates a KafkaConsumer with sensible defaults."""
    
    if not topics:
        raise ValueError('At least one topic must be provided')

    consumer = KafkaConsumer(
        *topics,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        auto_offset_reset=auto_offset_reset,
        enable_auto_commit=enable_auto_commit,
        value_deserializer=value_deserializer,
    )
    return consumer
