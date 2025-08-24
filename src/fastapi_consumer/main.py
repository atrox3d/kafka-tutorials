import logging
import os
import asyncio
import json

from kafka import KafkaConsumer
from fastapi import FastAPI






# variables defined in .devcontainer/.env
KAFKA_INTERNAL_PORT = os.environ.get("KAFKA_INTERNAL_PORT", "9092")
KAFKA_BROKER_URL = f'kafka:{KAFKA_INTERNAL_PORT}'
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "fastapi-topic")
KAFKA_CONSUMER_ID = 'fastapi-consumer'
CONSUMER_FASTAPI_PORT = os.environ.get("CONSUMER_FASTAPI_PORT", "8002")



logger = logging.getLogger('uvicorn.error')         # get uviconrn logger


def json_deserializer(value):
    """deserializer for kafka consumer"""
    
    logger.info(f'deserializing message: {value}')
    if value is None: 
        logger.info('value is None, returning')
        return None
    
    try:
        decoded = value.decode('utf-8')
        logger.info(f'decoded message: {decoded}')
        deserialized = json.loads(decoded)
        logger.info(f'deserialized message: {deserialized}')
        return deserialized
    except Exception as e:
        logger.error(f'unable to decode {e}, {value = }')
        return None


def create_kafka_consumer() -> KafkaConsumer:
    """create a kafka consumer"""
    
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BROKER_URL],
        group_id=KAFKA_CONSUMER_ID,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=json_deserializer
    )
    return consumer


async def poll_consumer(consumer: KafkaConsumer):
    """poll the kafka consumer if event is not set"""
    
    try:
        while not stop_polling_event.is_set():                                  # if false
            logger.info('trying to poll again')
            records = consumer.poll(                                            # records is a dict
                timeout_ms=5000,                                                # wait messages for 5 seconds
                max_records=250                                                 # max 250 messages for batch
            )
            logger.info(f'records: {records}')
            if records:                                                         # seems useless to me
                for record in records.values():                                 # get the values only
                    logger.info(f'record: {record}')
                    for message in record:                                      # every value is a list of kafka events
                        logger.info(f'message: {message}')
                        logger.info(f'message.value: {message.value}')
                        # deserialized_value = json.loads(message.value)        # this is wrong
                        deserialized_value = message.value                      # get the actual value
                        logger.info(f'deserialized_value: {deserialized_value}')
                        m = deserialized_value.get('message')                  # value is a dict
                        logger.info(f'{m = }')
                        logger.info(f'received message {m = } of {message.topic = }')
            await asyncio.sleep(5)                                             # yields control to the main loop for 5 seconds
    except Exception as e:
        logger.error(f'errors available {e}')
    finally:
        consumer.close()


stop_polling_event = asyncio.Event()                                            # set to false == polling ok
app = FastAPI()
tasklist = []


@app.get('/trigger')
async def trigger_polling():
    """trigger the polling"""
    
    if not tasklist:                                                            # if task is not empty we are already polling
        logger.info('tasklist is empty starting poll')
        stop_polling_event.clear()                                              # set to false: enable polling
        consumer = create_kafka_consumer()
        task = asyncio.create_task(poll_consumer(consumer))                     # schedule task
        tasklist.append(task)                                                   # add task to list 
        
        logger.info('poll started')
        return {'status': 'poll started'}
    return {'status': 'poll already running'}


@app.get('/stop')
async def stop_polling():
    """stop the polling"""
    
    logger.info('stopping poll')
    stop_polling_event.set()                                    # set to true 
    if tasklist:
        tasklist.pop()                                          # remove task
    
    logger.info('poll stopped')
    return {'status': 'poll stopped'}




if __name__ == "__main__":
    import uvicorn
    # The reload=True flag makes the server restart after code changes,
    # which is great for development.
    # Note: When using reload=True, uvicorn.run expects the app as a string.
    uvicorn.run(
        "main:app", 
        host="0.0.0.0", 
        port=int(CONSUMER_FASTAPI_PORT), 
        reload=True
    )
