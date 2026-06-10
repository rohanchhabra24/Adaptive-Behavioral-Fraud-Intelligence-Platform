import time, json, random, uuid, logging, os, threading
from kafka import KafkaProducer, KafkaConsumer
from faker import Faker

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
fake = Faker('en_IN')

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
TPS = int(os.getenv("TPS", "100"))

# Global control state
IS_RUNNING = False

producer = None
while producer is None:
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
    except:
        time.sleep(2)

def control_listener():
    global IS_RUNNING
    consumer = KafkaConsumer(
        "system-control",
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='earliest',
        group_id='generator-control'
    )
    logger.info("Listening for system-control signals...")
    for msg in consumer:
        try:
            payload = msg.value.decode('utf-8')
            if payload.strip() == '"START"' or payload.strip() == 'START':
                signal = 'START'
            elif payload.strip() == '"STOP"' or payload.strip() == 'STOP':
                signal = 'STOP'
            else:
                data = json.loads(payload)
                signal = data.get("command", "").strip().upper()
                
            if signal == "START":
                IS_RUNNING = True
                logger.info("✅ Received START signal - transactions flowing!")
            elif signal == "STOP":
                IS_RUNNING = False
                logger.info("⏹️  Received STOP signal - stopping transactions!")
        except Exception as e:
            logger.error(f"Error parsing control message: {e}")

def main():
    # Start the control listener in a background thread
    threading.Thread(target=control_listener, daemon=True).start()
    
    while True:
        if not IS_RUNNING:
            time.sleep(1)
            continue
            
        transaction = {
            "transaction_id": f"TXN-{uuid.uuid4().hex[:8]}",
            "user_id": f"U{random.randint(1,100):04d}",
            "amount": round(random.uniform(50000, 200000), 2) if random.random() < 0.05 else round(random.uniform(10, 5000), 2),
            "location": "International" if random.random() < 0.05 else random.choice(["Delhi", "Mumbai", "Bangalore"]),
            "timestamp": int(time.time() * 1000),
            "is_simulated_fraud": False
        }
        producer.send("transaction-raw", value=transaction)
        producer.flush()
        print(f"Sent transaction: {transaction['transaction_id']}")
        time.sleep(1.0 / TPS)

if __name__ == "__main__":
    main()
