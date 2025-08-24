import logging
import os
import asyncio
import json
import sys
from pathlib import Path
from tracemalloc import stop
from turtle import st
sys.path.insert(0, str(Path(__file__).resolve().parent))

from kafka import KafkaConsumer
from fastapi import FastAPI, BackgroundTasks


KAFKA_INTERNAL_PORT = os.environ.get("KAFKA_INTERNAL_PORT", "9092")
KAFKA_BROKER_URL = f'kafka:{KAFKA_INTERNAL_PORT}'
KAFKA_TOPIC = 'fastapi-topic'
KAFKA_CONSUMER_ID = 'fastapi-consumer'
CONSUMER_FASTAPI_PORT = os.environ.get("CONSUMER_FASTAPI_PORT", "8002")



logger = logging.getLogger('uvicorn.error')


def json_deserializer(value):
    logger.info(f'deserializing message: {value}')
    if value is None: 
        logger.info('value is None, returning')
        return
    
    try:
        decoded = value.decode('utf-8')
        logger.info(f'decoded message: {decoded}')
        deserialized = json.loads(decoded)
        logger.info(f'deserialized message: {deserialized}')
    except Exception as e:
        logger.error(f'unable to decode {e}, {value = }')
        return None


def create_kafka_consumer() -> KafkaConsumer:
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
    try:
        while not stop_polling_event.is_set():
            logger.info('trying to poll again')
            records = consumer.poll(timeout_ms=5000, max_records=250)
            logger.info(f'records: {records}')
            if records:
                for record in records.values():
                    logger.info(f'record: {record}')
                    for message in record:
                        logger.info(f'message: {message}')
                        logger.info(f'message.value: {message.value}')
                        deserialized_value = json.loads(message.value)
                        m = deserialized_value.get('message')
                        logger.info(f'received message {m = } of {message.topic = }')
            await asyncio.sleep(5)
    except Exception as e:
        logger.error(f'errors available {e}')
    finally:
        consumer.close()


stop_polling_event = asyncio.Event()
app = FastAPI(port=CONSUMER_FASTAPI_PORT)
tasklist = []


@app.get('/trigger')
async def trigger_polling():
    if not tasklist:
        logger.info('tasklist is empty starting poll')
        stop_polling_event.clear()
        consumer = create_kafka_consumer()
        task = asyncio.create_task(poll_consumer(consumer))
        tasklist.append(task)
        
        logger.info('poll started')
        return {'status': 'poll started'}
    return {'status': 'poll already running'}


@app.get('/stop')
async def stop_polling():
    logger.info('stopping poll')
    stop_polling_event.set()
    if tasklist:
        tasklist.pop()
    
    logger.info('poll stopped')
    return {'status': 'poll stopped'}
