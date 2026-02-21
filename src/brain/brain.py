import redis
import psycopg2
import os
import logging
import sys
from config import settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
)

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")

r = redis.Redis(host=REDIS_HOST, port=6379)
BRAIN_QUEUE_NAME = "brain_queue"

if r.ping():
    logging.info("Connected to Redis successfully!")
else:
    logging.error("Failed to connect to Redis.")
    sys.exit(1)

while True:
    _, job_data = r.blpop(BRAIN_QUEUE_NAME)
    
    logging.info(f"Received job data: {job_data}")
    