
import time
from kafka_helpers import create_consumer
from config import (
    ORDER_CONFIRMED_KAFKA_TOPIC,
    ANALYTICS_CLIENT_ID
)


consumer = create_consumer(ORDER_CONFIRMED_KAFKA_TOPIC, group_id=ANALYTICS_CLIENT_ID)

print('Listening to confirmed orders...')
total_orders = 0
total_revenue = 0
while True:
    for message in consumer:
        print('Updating analytics...')
        consumed_message = message.value
        total_cost = float(consumed_message['total_cost'])
        total_orders += 1
        total_revenue += total_cost
        print(f'Total orders: {total_orders}')
        print(f'Total revenue: {total_revenue}')
        consumer.commit()