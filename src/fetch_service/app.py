from fastapi import FastAPI, UploadFile, File
import redis
import json
import uvicorn
import boto3
import uuid
import os
from config import settings

app = FastAPI()
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")

r = redis.Redis(host=REDIS_HOST, port=6379)
s3_client = boto3.client('s3', aws_access_key_id=settings.ACCESS_KEY, aws_secret_access_key=settings.SECRET_ACCESS_KEY)

QUEUE_NAME = "ocr_queue"
BUCKET_NAME = "unstop-invoice"

@app.get("/")
def health_check():
    return {"status": "ok"}

@app.post("/upload")
def upload_data(file: UploadFile = File(...)):
    uuid_str = str(uuid.uuid4())
    s3_key = f"{file.filename}_{uuid_str}"
    s3_client.upload_fileobj(file.file, BUCKET_NAME, s3_key + ".pdf")
    job = {"invoice_id": s3_key + ".pdf"}
    r.rpush(QUEUE_NAME, json.dumps(job))
    return {"status": "queued", "invoice_id": s3_key}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)