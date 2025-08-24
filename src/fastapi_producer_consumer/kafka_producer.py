from kafka import KafkaProducer
from fastapi import HTTPException
from produce_schema import ProduceMessage
import json
import os
import logging


logger = logging.getLogger('uvicorn.error')

KAFKA_INTERNAL_PORT = os.environ.get("KAFKA_INTERNAL_PORT", "9092")
KAFKA_BROKER_URL = f'kafka:{KAFKA_INTERNAL_PORT}'
KAFKA_TOPIC = 'fastapi-topic'
PRODUCER_CLIENT_ID = 'fastapi-producer'


def serializer(message):
    logger.info(f'Serializing message: {message}')
    serialized = json.dumps(message).encode('utf-8')
    logger.info(f'Serialized message: {serialized}')
    return serialized


producer = KafkaProducer(
    api_version=(0, 8, 0),
    bootstrap_servers=[KAFKA_BROKER_URL],
    value_serializer=serializer,
    client_id=PRODUCER_CLIENT_ID
)


def produce_kafka_message(message: ProduceMessage):
    try:
        logger.info(f'Sending message to kafka topic {KAFKA_TOPIC}: "{message.message}"')
        producer.send(KAFKA_TOPIC, value=message.model_dump())
        producer.flush()
        logger.info('Message sent successfully.')
    except Exception as e:
        logger.error(f'Failed to send message to Kafka: {e}', exc_info=True)
        raise HTTPException(status_code=500, detail='Failed to send message to Kafka')
