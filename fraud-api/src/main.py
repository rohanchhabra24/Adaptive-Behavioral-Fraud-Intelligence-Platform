from fastapi import FastAPI, WebSocket
from fastapi.middleware.cors import CORSMiddleware
import asyncio, json, os, random
from kafka import KafkaConsumer
from src.routers import control

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

@app.websocket("/ws/metrics")
async def websocket_metrics(websocket: WebSocket):
    await websocket.accept()
    while True:
        await websocket.send_json({"type": "METRIC_UPDATE", "tps": random.randint(4800, 5200), "consumer_lag": random.randint(0, 5)})
        await asyncio.sleep(1)

@app.websocket("/ws/alerts")
async def websocket_alerts(websocket: WebSocket):
    await websocket.accept()
    consumer = KafkaConsumer("transaction-raw", bootstrap_servers=[os.getenv("KAFKA_BROKER", "kafka:29092")], value_deserializer=lambda x: json.loads(x.decode('utf-8')), auto_offset_reset='earliest', group_id=None)
    while True:
        records = consumer.poll(timeout_ms=100)
        for tp, messages in records.items():
            for msg in messages:
                if msg.value['amount'] > 50000 or msg.value['location'] == 'International':
                    await websocket.send_json({"type": "NEW_ALERT", "data": {"transaction_id": msg.value['transaction_id'], "risk_score": 85, "reason": "High Risk Detected", "severity": "HIGH RISK"}})
        await asyncio.sleep(0.5)

app.include_router(control.router, prefix="/api/v1/control", tags=["Control"])
