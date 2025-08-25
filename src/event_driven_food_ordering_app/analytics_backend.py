import json

from kafka import KafkaConsumer

from config import (
    ORDER_CONFIRMED_KAFKA_TOPIC,
    KAFKA_BROKER_URL
)


consumer = KafkaConsumer(
    ORDER_CONFIRMED_KAFKA_TOPIC, 
    bootstrap_servers=[KAFKA_BROKER_URL],
    # group_id="analytics-group",
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
        # consumer.commit()
