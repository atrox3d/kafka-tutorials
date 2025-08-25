import json
from math import prod
import time

from kafka import KafkaConsumer

from config import (
    ORDER_CONFIRMED_KAFKA_TOPIC,
    KAFKA_BROKER_URL
)


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
