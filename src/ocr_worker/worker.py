import redis
import json
from config import settings
import boto3
import os
import pytesseract
import logging
import sys
from pdf2image import convert_from_path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
)

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")

r = redis.Redis(host=REDIS_HOST, port=6379)
s3 = boto3.client('s3', aws_access_key_id=settings.ACCESS_KEY, aws_secret_access_key=settings.SECRET_ACCESS_KEY)
QUEUE_NAME = "ocr_queue"
BUKCET_NAME = "unstop-invoice"
temps_dir = "temp_invoices"
os.makedirs(temps_dir, exist_ok=True)

if r.ping():
    logging.info("Connected to Redis successfully.")
else:
    print("Failed to connect to Redis.")

def process_invoice(invoice_text:str):
    pass
    

while True:
    _, job_data = r.blpop(QUEUE_NAME)
    job = json.loads(job_data)

    invoice_id = job["invoice_id"]
    temp_file_path = os.path.join(temps_dir, invoice_id)
    logging.info(f"Processing invoice {invoice_id}")
    download_invoice = s3.download_file(BUKCET_NAME, invoice_id, temp_file_path)
    
    try:
        images = convert_from_path(temp_file_path)
        text = ""
        for image in images:
            text += pytesseract.image_to_string(image) + "\n"
            
        r.setex("test_key", 30, "it successfully works")
        print(f"Extracted text for {invoice_id}:\n{text[:]}...")
        logging.info(f"OCR completed for {invoice_id}")
    except Exception as e:
        print(f"Error processing {invoice_id}: {e}")
    finally:
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)
            print(f"Removed temp file for {invoice_id}")

    print(f"OCR completed for {invoice_id}")