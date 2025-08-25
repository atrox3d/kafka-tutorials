import json
from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from typing import Callable, Any

from config import (
    KAFKA_BROKER_URL,
    ADMIN_CLIENT_ID,
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


def create_admin_client(
    bootstrap_servers: str | list[str] = KAFKA_BROKER_URL,
    client_id: str = ADMIN_CLIENT_ID,
    request_timeout_ms: int = 5000,
) -> KafkaAdminClient:
    """Creates a KafkaAdminClient with sensible defaults."""
    admin_client = KafkaAdminClient(
        bootstrap_servers=bootstrap_servers,
        client_id=client_id,
        request_timeout_ms=request_timeout_ms  # Add a timeout for robustness
    )
    return admin_client


def delete_kafka_topics(
    *topics: str,
    admin_client: KafkaAdminClient = create_admin_client(), 
):
    """
    Connects to Kafka and deletes a list of specified topics.
    """
    # logging.info(f"Connecting to Kafka at {KAFKA_BROKER_URL}...")
    admin_client.delete_topics(topics=topics, timeout_ms=5000)
