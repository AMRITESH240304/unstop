import redis
import json
from config import settings
import boto3
import os
import pytesseract
import logging
import sys
import psycopg2
import uuid
from pdf2image import convert_from_path
from agent.llm_check import process_invoice

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
)
s3_client = boto3.client('s3', aws_access_key_id=settings.ACCESS_KEY, aws_secret_access_key=settings.SECRET_ACCESS_KEY)
BUCKET_NAME = "unstop-invoice"

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

def save_parsed_invoice(invoice_id, validation_result):
    data = validation_result["invoice_data"]
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO invoices (
            id, invoice_number, invoice_date, vendor_name,
            subtotal, tax, total, status, raw_text
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET
            invoice_number = EXCLUDED.invoice_number,
            invoice_date = EXCLUDED.invoice_date,
            vendor_name = EXCLUDED.vendor_name,
            subtotal = EXCLUDED.subtotal,
            tax = EXCLUDED.tax,
            total = EXCLUDED.total,
            raw_text = EXCLUDED.raw_text,
            status = EXCLUDED.status
    """, (
        invoice_id,
        data.get("invoice_number"),
        data.get("invoice_date"),
        data.get("vendor_name"),
        data.get("subtotal"),
        data.get("tax"),
        data.get("total"),
        "PARSED",
        validation_result.get("raw_text")
    ))

    cur.execute("DELETE FROM invoice_line_items WHERE invoice_id = %s", (invoice_id,))

    for item in data.get("line_items", []):
        cur.execute("""
            INSERT INTO invoice_line_items (
                id, invoice_id, description, quantity, unit_price, total
            )
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            str(uuid.uuid4()),
            invoice_id,
            item.get("description"),
            item.get("quantity"),
            item.get("unit_price"),
            item.get("total")
        ))

    conn.commit()
    cur.close()

conn.commit()
cur.close()

logging.info("Database setup completed successfully.")

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")

r = redis.Redis(host=REDIS_HOST, port=6379)
s3 = boto3.client('s3', aws_access_key_id=settings.ACCESS_KEY, aws_secret_access_key=settings.SECRET_ACCESS_KEY)
QUEUE_NAME = "ocr_queue"
BUKCET_NAME = "unstop-invoice"
temps_dir = "temp_invoices"
BRAIN_QUEUE_NAME = "brain_queue"
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
        if validation.get("validation", {}).get("is_valid"):
            new_invoiceID = str(uuid.uuid4())
            save_parsed_invoice(new_invoiceID, validation)
            logging.info(f"Saved parsed invoice {invoice_id} to DB")
        else:
            logging.warning(f"Invoice {invoice_id} failed validation")
            
        logging.info(f"OCR completed for {invoice_id}")
    except Exception as e:
        logging.warning(f"Error processing {invoice_id}: {e}")
    finally:
        if os.path.exists(temp_file_path):
            s3_client.delete_object(Bucket=BUKCET_NAME, Key=invoice_id)
            r.rpush(BRAIN_QUEUE_NAME, json.dumps({"invoice_id": new_invoiceID, "status": "processed"}))
            os.remove(temp_file_path)
            logging.info(f"Removed temp file for {invoice_id}")
            logging.info(f"Deleted {invoice_id} from S3")
            logging.info(f"Pushed {new_invoiceID} to brain queue")
            print(f"Removed temp file for {invoice_id}")

    print(f"OCR completed for {invoice_id}")