from fastapi import FastAPI, BackgroundTasks
from kafka.admin import KafkaAdminClient, NewTopic
from contextlib import asynccontextmanager
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import os
from kafka_producer import produce_message
from produce_schema import ProduceMessage


KAFKA_INTERNAL_PORT = os.environ.get("KAFKA_INTERNAL_PORT", "9092")
KAFKA_BROKER_URL = f'kafka:{KAFKA_INTERNAL_PORT}'
KAFKA_TOPIC = 'fastapi-topic'
KAFKA_ADMIN_CLIENT = 'fastapi-admin-client'


@asynccontextmanager
async def lifespan(app: FastAPI):
    
    admin_client = KafkaAdminClient(
        bootstrap_servers=[KAFKA_BROKER_URL],
        client_id=KAFKA_ADMIN_CLIENT
    )
    
    if not KAFKA_TOPIC in admin_client.list_topics():
        admin_client.create_topics(
            new_topics=[
                NewTopic(
                    name=KAFKA_TOPIC, 
                    num_partitions=1, 
                    replication_factor=1
                )
            ],
            validate_only=False
        )
        # admin_client.delete_topics(topics=[KAFKA_TOPIC])
    
    yield   # separation point between start and stop application in lifespan
    

app = FastAPI(lifespan=lifespan)


@app.post('/produce/message')
async def produce_message(messageRequest: ProduceMessage, background_tasks: BackgroundTasks):
    background_tasks.add_task(produce_message, messageRequest)
    return {'message': 'Message received, thank you for sending a message'}
