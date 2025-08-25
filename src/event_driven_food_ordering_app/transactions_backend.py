import time
from kafka_helpers import create_producer, create_consumer
from config import (
    ORDER_KAFKA_TOPIC,
    ORDER_CONFIRMED_KAFKA_TOPIC,
    TRANSACTION_CLIENT_ID
)

consumer = create_consumer(ORDER_KAFKA_TOPIC, group_id=TRANSACTION_CLIENT_ID)
producer = create_producer()

print('Listening to orders...')
while True:
    for message in consumer:
        print('Ongoing transaction...')
        # consumed_message = json.loads(message.value.decode())
        consumed_message = message.value
        print(consumed_message)
        
        customer_id = consumed_message['customer_id']
        total_cost = consumed_message['total_cost']
        data = {
            'order_id': consumed_message['order_id'],
            'items': consumed_message['items'],
            'customer_id': customer_id,
            'customer_email': f'{customer_id}@gmail.com',
            'total_cost': total_cost
        }
        print('Successful transaction, sending confirmation...')
        # producer.send(ORDER_CONFIRMED_KAFKA_TOPIC, json.dumps(data).encode("utf-8"))
        producer.send(ORDER_CONFIRMED_KAFKA_TOPIC, data)
        print('Confirmation sent.')
        consumer.commit()
