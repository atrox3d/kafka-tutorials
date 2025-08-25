import time
import random

from kafka_helpers import create_producer
from config import (
    ORDER_KAFKA_TOPIC, 
    ORDER_LIMIT, 
    ORDER_START_SECONDS, 
    ORDER_WAIT_SECONDS, 
)


producer = create_producer()

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

    producer.send(
        ORDER_KAFKA_TOPIC, 
        # json.dumps(data).encode("utf-8")
        data
    )
    print(f'order sent: {data}')
    time.sleep(ORDER_WAIT_SECONDS)

print('All orders sent')
