import json
from math import prod
import time
import os
import random

from kafka import KafkaConsumer, KafkaProducer

ORDER_KAFKA_TOPIC = "order_details"
ORDER_CONFIRMED_KAFKA_TOPIC = "order_confirmed"
KAFKA_INTERNAL_PORT = os.environ.get("KAFKA_INTERNAL_PORT", "9092")
KAFKA_BROKER_URL = f'kafka:{KAFKA_INTERNAL_PORT}'
# PRODUCER_CLIENT_ID = 'fastapi-producer'


consumer = KafkaConsumer(
    ORDER_KAFKA_TOPIC, 
    bootstrap_servers=[KAFKA_BROKER_URL],
    auto_offset_reset='earliest',
    enable_auto_commit=False,
)
producer = KafkaProducer(bootstrap_servers=[KAFKA_BROKER_URL])

print('Listening to orders...')
while True:
    for message in consumer:
        print('Ongoing transaction...')
        consumed_message = json.loads(message.value.decode())
        print(consumed_message)
        
        user_id = consumed_message['user_id']
        total_cost = consumed_message['total_cost']
        data = {
            'customer_id': user_id,
            'customer_email': f'{user_id}@gmail.com',
            'total_cost': total_cost
        }
        print('Successful transaction, sending confirmation...')
        producer.send(ORDER_CONFIRMED_KAFKA_TOPIC, json.dumps(data).encode("utf-8"))
