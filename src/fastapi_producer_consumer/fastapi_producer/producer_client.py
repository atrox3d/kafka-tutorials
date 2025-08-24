import requests
import os


PRODUCER_FASTAPI_PORT = os.environ.get("PRODUCER_FASTAPI_PORT", "8001")
PRODUCER_FASTAPI_HOST = 'localhost'
PRODUCER_FASTAPI_URL = f'http://{PRODUCER_FASTAPI_HOST}:{PRODUCER_FASTAPI_PORT}'
PRODUCER_FASTAPI_PATH = 'produce/message'
PRODUCER_FASTAPI_ENDPOINT = f'{PRODUCER_FASTAPI_URL}/{PRODUCER_FASTAPI_PATH}'






def send_message_to_kafka(message: str):
    response = requests.post(
        PRODUCER_FASTAPI_ENDPOINT, 
        json={'message': message}
    )
    if response.status_code == 200:
        print('Message sent successfully')
    else:
        print(f'Failed to send message. Status code: {response.status_code}')


if __name__ == "__main__":
    send_message_to_kafka('testing testing')