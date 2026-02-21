import redis
import psycopg2
import os
import logging
import sys
import json
from config import settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
)

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST", "localhost"),
    port=settings.POSTGRES_PORT,
    dbname=settings.POSTGRES_DB,
    user=settings.POSTGRES_USER,
    password=settings.POSTGRES_PASSWORD
)

cur = conn.cursor()
cur.execute("""
    CREATE TABLE IF NOT EXISTS purchase_orders (
    id UUID PRIMARY KEY,
    po_number VARCHAR(100) UNIQUE,
    vendor_name VARCHAR(255),
    total NUMERIC(12,2),
    status VARCHAR(50)
);
""")

conn.commit()

def validation_check(invoice_data):
    po_number = invoice_data.get("invoice_number")
    vendor_name = invoice_data.get("vendor_name")
    total = invoice_data.get("total")
    cur.execute("SELECT id FROM purchase_orders WHERE po_number = %s AND vendor_name = %s AND total = %s", 
                (po_number, vendor_name, total))
    result = cur.fetchone()
    if result:
        return {"status": "valid", "po_id": str(result[0]), "invoice_data": invoice_data}
    else:
        return {"status": "invalid", "invoice_data": invoice_data}

def fetch_invoice_data(invoice_id):
    try:
        cur.execute("""
            SELECT 
                i.id, i.invoice_number, i.invoice_date, i.vendor_name, 
                i.subtotal, i.tax, i.total, i.status,
                li.description, li.quantity, li.unit_price, li.total as li_total
            FROM invoices i
            LEFT JOIN invoice_line_items li ON i.id = li.invoice_id
            WHERE i.id = %s;
        """, (invoice_id,))
        
        rows = cur.fetchall()
        
        logging.info(f"fetched {rows} for invoice_id {invoice_id}")
        
        if not rows:
            return None

        first_row = rows[0]
        invoice = {
            "id": first_row[0],
            "invoice_number": first_row[1],
            "invoice_date": str(first_row[2]),
            "vendor_name": first_row[3],
            "subtotal": float(first_row[4]),
            "tax": float(first_row[5]),
            "total": float(first_row[6]),
            "status": first_row[7],
            "line_items": []
        }

        for row in rows:
            if row[8]:  
                invoice["line_items"].append({
                    "description": row[8],
                    "quantity": float(row[9]),
                    "unit_price": float(row[10]),
                    "total": float(row[11])
                })
        
        return invoice

    except Exception as e:
        logging.error(f"Database error: {e}")
        return None

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
    
    job = json.loads(job_data)
    invoice_id = job["invoice_id"]
    
    invoice_detail = fetch_invoice_data(invoice_id)
    
    if invoice_detail:
        validation_result = validation_check(invoice_detail)
        logging.info(f"Validation result for invoice {invoice_id}: {validation_result}")
        
        if validation_result["status"] == "valid":
            logging.info(f"Invoice {invoice_id} is valid. PO ID: {validation_result['po_id']}")
        else:
            logging.warning(f"Invoice {invoice_id} is invalid.")
    else:
        logging.error(f"No invoice data found for ID: {invoice_id}")