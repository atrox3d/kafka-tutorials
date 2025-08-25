import time

from db.orders import update_orders
from db.transactions import update_transactions

from kafka_helpers import create_consumer
from config import (
    ORDER_KAFKA_TOPIC,
    ORDER_CONFIRMED_KAFKA_TOPIC,
    DB_CONSUMER_GROUP_ID,
    ORDERS_DATA_PATH,
    TRANSACTIONS_DATA_PATH,
    EMAIL_DATA_PATH,
    ANALYTICS_DATA_PATH,
)


consumer = create_consumer(
    ORDER_KAFKA_TOPIC,
    ORDER_CONFIRMED_KAFKA_TOPIC,
    group_id=DB_CONSUMER_GROUP_ID
)

print("DB consumer is listening...")
while True:
    for message in consumer:
        if message.topic == ORDER_KAFKA_TOPIC:
            order_data = message.value
            print(f"DB: Received new order. INSERTING: {order_data}")
            # In a real app: db_connection.execute("INSERT ...", order_data)
            update_orders(ORDERS_DATA_PATH, order_data)
        elif message.topic == ORDER_CONFIRMED_KAFKA_TOPIC:
            confirmation_data = message.value
            print(f"DB: Received confirmation. UPDATING: {confirmation_data}")
            update_transactions(TRANSACTIONS_DATA_PATH, confirmation_data)
        consumer.commit()
