from fastapi import FastAPI, BackgroundTasks
from kafka.admin import KafkaAdminClient, NewTopic
from contextlib import asynccontextmanager
import os
import sys
from pathlib import Path
import logging

sys.path.insert(0, str(Path(__file__).resolve().parent))
from kafka_producer import produce_kafka_message
from produce_schema import ProduceMessage


# variables defined in .devcontainer/.env
KAFKA_INTERNAL_PORT = os.environ.get("KAFKA_INTERNAL_PORT", "9092")
KAFKA_BROKER_URL = f'kafka:{KAFKA_INTERNAL_PORT}'
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "fastapi-topic")
KAFKA_ADMIN_CLIENT = 'fastapi-admin-client'
PRODUCER_FASTAPI_PORT = os.environ.get("PRODUCER_FASTAPI_PORT", "8001")


logger = logging.getLogger('uvicorn.error')         # get uviconrn logger


@asynccontextmanager
async def lifespan(app: FastAPI):
    """create kafka admin client on startup"""
    
    logger.info(
        f'creating admin_client: '
        f'bootstrap_servers={KAFKA_BROKER_URL}, '
        'client_id={KAFKA_ADMIN_CLIENT}'
    )
    
    admin_client = KafkaAdminClient(                                    # create admin client
        bootstrap_servers=[KAFKA_BROKER_URL],
        client_id=KAFKA_ADMIN_CLIENT
    )
    
    if not KAFKA_TOPIC in admin_client.list_topics():                   # check if topic exists
        logger.info(f"Topic '{KAFKA_TOPIC}' not found. Creating it.")
        admin_client.create_topics(
            new_topics=[
                NewTopic(
                    name=KAFKA_TOPIC, 
                    num_partitions=1, 
                    replication_factor=1
                )
            ],
            validate_only=False                                         # If True, don't actually create new topics
        )
    else:
        logger.info(f"Topic '{KAFKA_TOPIC}' already exists.")
        # admin_client.delete_topics(topics=[KAFKA_TOPIC])              # how to delete topic
    
    yield                                                               # separation point between start and stop application in lifespan
    

app = FastAPI(lifespan=lifespan)


@app.post('/produce/message')
async def produce_message(messageRequest: ProduceMessage, background_tasks: BackgroundTasks):
    """produce a message to kafka"""
    
    logger.info(
        f'Adding background task to produce message: "{messageRequest.message}"'
    )
    background_tasks.add_task(                                         # add task to background tasks           
        produce_kafka_message,                                         # function to run       
        messageRequest                                                 # arguments to function  
    )
    return {'message': 'Message received, thank you for sending a message'}


if __name__ == "__main__":
    import uvicorn
    # The reload=True flag makes the server restart after code changes,
    # which is great for development.
    # Note: When using reload=True, uvicorn.run expects the app as a string.
    uvicorn.run(
        "main:app", 
        host="0.0.0.0", 
        port=int(PRODUCER_FASTAPI_PORT), 
        reload=True
    )
