import os
from fastapi import APIRouter
from kafka import KafkaProducer

router = APIRouter()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")

producer = None

def get_producer():
    global producer
    if producer is None:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda x: x.encode('utf-8')
        )
    return producer

@router.post("/start")
def start_generator():
    get_producer().send("system-control", value="START")
    return {"status": "started"}

@router.post("/stop")
def stop_generator():
    get_producer().send("system-control", value="STOP")
    return {"status": "stopped"}
