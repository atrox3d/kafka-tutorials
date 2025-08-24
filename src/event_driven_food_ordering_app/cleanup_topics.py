import os
from kafka.admin import KafkaAdminClient
from kafka.errors import UnknownTopicOrPartitionError
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Topics to delete
TOPICS_TO_DELETE = ["order_details", "order_confirmed", "fastapi-topic"]

KAFKA_INTERNAL_PORT = os.environ.get("KAFKA_INTERNAL_PORT", "9092")
KAFKA_BROKER_URL = f'kafka:{KAFKA_INTERNAL_PORT}'
ADMIN_CLIENT_ID = 'cleanup-admin-client'

def delete_kafka_topics():
    """
    Connects to Kafka and deletes a list of specified topics.
    """
    logging.info(f"Connecting to Kafka at {KAFKA_BROKER_URL}...")
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=[KAFKA_BROKER_URL],
            client_id=ADMIN_CLIENT_ID,
            request_timeout_ms=5000  # Add a timeout for robustness
        )
        logging.info(f"Successfully connected. Attempting to delete topics: {TOPICS_TO_DELETE}")
        
        admin_client.delete_topics(topics=TOPICS_TO_DELETE, timeout_ms=5000)
        
        logging.info("Delete command sent for all specified topics. It may take a moment for the broker to complete the deletion.")
    except Exception as e:
        logging.error(f"An error occurred while trying to delete topics: {e}", exc_info=True)

if __name__ == "__main__":
    delete_kafka_topics()

