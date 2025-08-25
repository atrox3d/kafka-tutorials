import json
from math import prod
import time
import random

from kafka import KafkaProducer

from config import (
    ORDER_KAFKA_TOPIC, 
    ORDER_LIMIT, 
    ORDER_START_SECONDS, 
    ORDER_WAIT_SECONDS, 
    KAFKA_BROKER_URL
)


producer = KafkaProducer(bootstrap_servers=[KAFKA_BROKER_URL])

print(f'Generating orders after {ORDER_START_SECONDS} seconds')
print(f'Will generate one unique order every {ORDER_WAIT_SECONDS} seconds')
time.sleep(ORDER_START_SECONDS)

for i in range(ORDER_LIMIT):
    data = {
        "order_id": i,
        "user_id": f"user_{i}",
        "total_cost": i *  5,
        "items": random.choice('hamburger salad hotdog sandwich'.split())
    }

    producer.send(ORDER_KAFKA_TOPIC, json.dumps(data).encode("utf-8"))
    print(f'order sent: {data}')
    time.sleep(ORDER_WAIT_SECONDS)

print('All orders sent')
