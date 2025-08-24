from kafka import KafkaProducer
from fastapi import HTTPException
from produce_schema import ProduceMessage
import json
import os


KAFKA_INTERNAL_PORT = os.environ.get("KAFKA_INTERNAL_PORT", "9092")
KAFKA_BROKER_URL = f'kafka:{KAFKA_INTERNAL_PORT}'
KAFKA_TOPIC = 'fastapi-topic'
PRODUCER_CLIENT_ID = 'fastapi-producer'


def serializer(message):
    return json.dumps(message).encode('utf-8')


producer = KafkaProducer(
    api_version=(0, 8, 0),
    bootstrap_servers=[KAFKA_BROKER_URL],
    value_serializer=serializer,
    client_id=PRODUCER_CLIENT_ID
)


def produce_message(message: ProduceMessage):
    try:
        producer.send(KAFKA_TOPIC, json.dumps({'message': message.message}))
        producer.flush()
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail='Failed to send message to Kafka')

