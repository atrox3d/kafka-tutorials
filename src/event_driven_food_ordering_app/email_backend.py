import time

from kafka_helpers import create_consumer
from config import (
    ORDER_CONFIRMED_KAFKA_TOPIC,
)

consumer = create_consumer(ORDER_CONFIRMED_KAFKA_TOPIC)

email_sent = set()
print('Listening to confirmed orders...')
while True:
    for message in consumer:
        print('sending mail...')
        # consumed_message = json.loads(message.value.decode())
        consumed_message = message.value
        customer_email = consumed_message['customer_email']
        print(f'Sending email to {customer_email}')
        email_sent.add(customer_email)
        print(f'Sent {len(email_sent)} unique emails')
        time.sleep(1)
        # consumer.commit()
