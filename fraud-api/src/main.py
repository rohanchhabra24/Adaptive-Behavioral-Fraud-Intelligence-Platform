from fastapi import FastAPI, WebSocket
from fastapi.middleware.cors import CORSMiddleware
import asyncio, json, os, time, threading
from kafka import KafkaConsumer
from src.routers import control

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])
app.include_router(control.router, prefix="/api/v1/control", tags=["Control"])

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")

# Global metrics
metrics_state = {"total_processed": 0, "current_tps": 0, "lag": 0}

def metrics_calculator():
    """Background thread to calculate True TPS by listening to transaction-raw."""
    consumer = KafkaConsumer("transaction-raw", bootstrap_servers=[KAFKA_BROKER], auto_offset_reset='latest')
    last_count = 0
    last_time = time.time()
    
    # Non-blocking poll loop to calculate TPS
    while True:
        records = consumer.poll(timeout_ms=1000)
        batch_count = sum(len(msgs) for msgs in records.values())
        metrics_state["total_processed"] += batch_count
        
        current_time = time.time()
        elapsed = current_time - last_time
        if elapsed >= 1.0:
            metrics_state["current_tps"] = int((metrics_state["total_processed"] - last_count) / elapsed)
            metrics_state["lag"] = 0 # In a real system, you'd calculate true consumer lag here
            last_count = metrics_state["total_processed"]
            last_time = current_time

threading.Thread(target=metrics_calculator, daemon=True).start()

@app.websocket("/ws/metrics")
async def websocket_metrics(websocket: WebSocket):
    await websocket.accept()
    while True:
        await websocket.send_json({
            "type": "METRIC_UPDATE", 
            "tps": metrics_state["current_tps"], 
            "consumer_lag": metrics_state["lag"]
        })
        await asyncio.sleep(1)

@app.websocket("/ws/alerts")
async def websocket_alerts(websocket: WebSocket):
    await websocket.accept()
    # Listen to the ACTUAL Flink output topic!
    consumer = KafkaConsumer(
        "fraud-alerts", 
        bootstrap_servers=[KAFKA_BROKER], 
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    while True:
        records = consumer.poll(timeout_ms=100)
        for tp, messages in records.items():
            for msg in messages:
                # msg.value contains actual risk_score and severity calculated by Flink!
                await websocket.send_json({"type": "NEW_ALERT", "data": msg.value})
        await asyncio.sleep(0.5)
