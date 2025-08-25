import time
import random

from db.orders import get_next_order_id
from kafka_helpers import create_producer, delete_kafka_topics, create_admin_client
from config import (
    ORDER_KAFKA_TOPIC,
    ALL_KAFKA_TOPICS,
    DEFAULT_ORDER_LIMIT,
    DEFAULT_ORDER_START_SECONDS,
    DEFAULT_ORDER_WAIT_SECONDS,
    ORDERS_DATA_PATH,
)

ORDER_LIMIT = 5
ORDER_START_SECONDS = 1
ORDER_WAIT_SECONDS = .5


producer = create_producer()

# print('Deleting existing topics...')
# admin_client = create_admin_client()
# delete_kafka_topics(*ALL_KAFKA_TOPICS, admin_client=admin_client)
# print('Topics deleted')

print(f'Generating orders after {ORDER_START_SECONDS} seconds')
print(f'Will generate one unique order every {ORDER_WAIT_SECONDS} seconds')
time.sleep(ORDER_START_SECONDS)

next_order = get_next_order_id(ORDERS_DATA_PATH)

for i in range(next_order, next_order + ORDER_LIMIT):
    data = {
        "order_id": i,
        "customer_id": f"user_{i}",
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
