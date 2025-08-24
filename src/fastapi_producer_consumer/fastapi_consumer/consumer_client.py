import requests
import os


CONSUMER_FASTAPI_PORT = os.environ.get("CONSUMER_FASTAPI_PORT", "8002")
CONSUMER_FASTAPI_HOST = 'localhost'
CONSUMER_FASTAPI_URL = f'http://{CONSUMER_FASTAPI_HOST}:{CONSUMER_FASTAPI_PORT}'
CONSUMER_FASTAPI_TRIGGER_PATH = 'trigger'
CONSUMER_FASTAPI_TRIGGER_ENDPOINT = f'{CONSUMER_FASTAPI_URL}/{CONSUMER_FASTAPI_TRIGGER_PATH}'
CONSUMER_FASTAPI_STOP_PATH = 'stop'
CONSUMER_FASTAPI_STOP_ENDPOINT = f'{CONSUMER_FASTAPI_URL}/{CONSUMER_FASTAPI_STOP_PATH}'



def trigger_consumer():
    response = requests.get(
        CONSUMER_FASTAPI_TRIGGER_ENDPOINT, 
    )
    if response.status_code == 200:
        print('Poll triggered successfully')
    else:
        print(f'Failed to trigger poll. Status code: {response.status_code}')


if __name__ == "__main__":
    trigger_consumer()
