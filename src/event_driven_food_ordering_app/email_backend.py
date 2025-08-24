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
    # group_id="email-group",
    auto_offset_reset='earliest',
    enable_auto_commit=False,
)
# producer = KafkaProducer(bootstrap_servers=[KAFKA_BROKER_URL])

email_sent = set()
print('Listening to confirmed orders...')
while True:
    for message in consumer:
        print('sending mail...')
        consumed_message = json.loads(message.value.decode())
        customer_email = consumed_message['customer_email']
        print(f'Sending email to {customer_email}')
        email_sent.add(customer_email)
        print(f'Sent {len(email_sent)} unique emails')
        time.sleep(1)
        # consumer.commit()
