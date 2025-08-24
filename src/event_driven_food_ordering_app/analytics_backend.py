import json
from math import prod
import time
import os
import random

from kafka import KafkaConsumer, KafkaProducer

# ORDER_KAFKA_TOPIC = "order_details"
ORDER_CONFIRMED_KAFKA_TOPIC = "order_confirmed"
KAFKA_INTERNAL_PORT = os.environ.get("KAFKA_INTERNAL_PORT", "9092")
KAFKA_BROKER_URL = f'kafka:{KAFKA_INTERNAL_PORT}'
# PRODUCER_CLIENT_ID = 'fastapi-producer'


consumer = KafkaConsumer(
    ORDER_CONFIRMED_KAFKA_TOPIC, 
    bootstrap_servers=[KAFKA_BROKER_URL],
    auto_offset_reset='earliest',
    enable_auto_commit=False,
)
# producer = KafkaProducer(bootstrap_servers=[KAFKA_BROKER_URL])

print('Listening to confirmed orders...')
total_orders = 0
total_revenue = 0
while True:
    for message in consumer:
        print('Updating analytics...')
        consumed_message = json.loads(message.value.decode())
        total_cost = float(consumed_message['total_cost'])
        total_orders += 1
        total_revenue += total_cost
        print(f'Total orders: {total_orders}')
        print(f'Total revenue: {total_revenue}')

