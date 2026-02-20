import redis
import json
from config import settings
import boto3
import os
import pytesseract
import logging
import sys
import psycopg2
from pdf2image import convert_from_path
from agent.llm_check import process_invoice

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
)

conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST", "localhost"),
    port=settings.POSTGRES_PORT,
    dbname=settings.POSTGRES_DB,
    user=settings.POSTGRES_USER,
    password=settings.POSTGRES_PASSWORD
)

cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS invoices (
    id UUID PRIMARY KEY,
    invoice_number VARCHAR(100),
    invoice_date DATE,
    vendor_name VARCHAR(255),
    subtotal NUMERIC(12,2),
    tax NUMERIC(12,2),
    total NUMERIC(12,2),
    status VARCHAR(50),
    raw_text TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS invoice_line_items (
    id UUID PRIMARY KEY,
    invoice_id UUID REFERENCES invoices(id) ON DELETE CASCADE,
    description TEXT,
    quantity NUMERIC(10,2),
    unit_price NUMERIC(12,2),
    total NUMERIC(12,2)
);
""")

conn.commit()
cur.close()
conn.close()

logging.info("Database setup completed successfully.")

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
            
        validation = process_invoice(text)

        print("Validation Result:", validation)
            
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