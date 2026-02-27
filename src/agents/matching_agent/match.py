import redis
import os
import json
import logging
import sys
import psycopg2
from config import settings
from langchain_core.tools import tool
from langchain.agents import create_agent
from langchain_google_genai import ChatGoogleGenerativeAI

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
)

model = ChatGoogleGenerativeAI(
    model="gemini-flash-latest",
    temperature=0.2,
    google_api_key=settings.GEMINI_API_KEY,
)

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")

conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST", "localhost"),
    port=settings.POSTGRES_PORT,
    dbname=settings.POSTGRES_DB,
    user=settings.POSTGRES_USER,
    password=settings.POSTGRES_PASSWORD
)

r = redis.Redis(host=REDIS_HOST, port=6379)
ESCALATE_QUEUE_NAME = "escalate_queue"
STATE_MACHINE_QUEUE_NAME = "state_machine_queue"

if r.ping():
    logging.info("Connected to Redis successfully!")
else:
    logging.error("Failed to connect to Redis.")
    
    
@tool
def getPOTool(po_number: str):
    """Fetch purchase order details by PO number."""
    query = """
    SELECT id, po_number, vendor_name, total, status
    FROM purchase_orders
    WHERE po_number = %s;
    """
    with conn.cursor() as cur:
        cur.execute(query, (po_number,))
        row = cur.fetchone()

    if not row:
        return json.dumps({"exists": False})

    return json.dumps({
        "exists": True,
        "id": str(row[0]),
        "po_number": row[1],
        "vendor_name": row[2],
        "total": float(row[3]),
        "status": row[4]
    })

@tool
def getPOLineItemsTool(po_id: str):
    """Fetch line items for a given purchase order ID."""
    query = """
    SELECT description, quantity, unit_price, total
    FROM po_line_items
    WHERE po_id = %s;
    """
    with conn.cursor() as cur:
        cur.execute(query, (po_id,))
        rows = cur.fetchall()

    results = [
        {
            "description": r[0],
            "quantity": float(r[1]),
            "unit_price": float(r[2]),
            "total": float(r[3]),
        }
        for r in rows
    ]

    return json.dumps(results)

@tool
def updateInvoiceStatusTool(invoice_id: str, value: str):
    """Update the status of an invoice by invoice ID."""
    query = """
    UPDATE invoices
    SET status = %s
    WHERE id = %s;
    """
    with conn.cursor() as cur:
        cur.execute(query, (value, invoice_id))
        conn.commit()

    return f"Invoice {invoice_id} updated to {value}"

    
@tool
def getInvoiceTool(invoice_id: str):
    """Fetch invoice details and line items by invoice ID."""
    query = """
    SELECT i.invoice_number, i.vendor_name, i.invoice_date,
           li.quantity, li.unit_price, li.description, li.total AS item_total
    FROM invoices i
    JOIN invoice_line_items li ON i.id = li.invoice_id
    WHERE i.id = %s;
    """
    with conn.cursor() as cur:
        cur.execute(query, (invoice_id,))
        cols = [desc[0] for desc in cur.description]
        rows = cur.fetchall()

    results = [dict(zip(cols, row)) for row in rows]
    return json.dumps(results, default=str)


tools = [
    getInvoiceTool,
    getPOTool,
    getPOLineItemsTool,
    updateInvoiceStatusTool
]

agent = create_agent(
    model=model,
    tools=tools,
)

def run_matching_agent(invoice_id: str):
    prompt = f"""
You are a Matching Agent for Accounts Payable.

Steps:
1. Fetch invoice using getInvoiceTool.
2. Fetch PO using getPOTool using invoice_number.
3. If PO does not exist → update invoice status to ESCALATED.
4. If PO status is not APPROVED → update invoice status to ESCALATED.
5. Compare invoice total with PO total.
6. Fetch PO line items.
7. Compare quantities, unit_price, and total per line item.
8. If everything matches → update invoice status to MATCHED.
9. Else → update invoice status to MISMATCH.

Invoice ID: {invoice_id}
"""

    inputs = {"messages": [{"role": "user", "content": prompt}]}
    result = agent.invoke(inputs)
    return result

def query_invoice_status(invoice_id: str):
    query = "SELECT status FROM invoices WHERE id = %s"
    with conn.cursor() as cur:
        cur.execute(query, (invoice_id,))
        row = cur.fetchone()
    return row[0] if row else None

while True:
    _, invoice_id = r.brpop(ESCALATE_QUEUE_NAME)
    invoice_id = invoice_id.decode()

    logging.info(f"Processing invoice {invoice_id} in Matching Agent")

    result = run_matching_agent(invoice_id)
    query_result = query_invoice_status(invoice_id)
    about_invoice = {"invoice_id": invoice_id, "result": result, "status": query_result}
    r.lpush(STATE_MACHINE_QUEUE_NAME, json.dumps(about_invoice))
