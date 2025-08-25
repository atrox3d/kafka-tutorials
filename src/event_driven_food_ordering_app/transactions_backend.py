import json
from math import prod

from kafka import KafkaConsumer, KafkaProducer

from config import (
    ORDER_KAFKA_TOPIC,
    ORDER_CONFIRMED_KAFKA_TOPIC,
    KAFKA_BROKER_URL
)


consumer = KafkaConsumer(
    ORDER_KAFKA_TOPIC, 
    bootstrap_servers=[KAFKA_BROKER_URL],
    # group_id="transactions-group",
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
        # consumer.commit()
