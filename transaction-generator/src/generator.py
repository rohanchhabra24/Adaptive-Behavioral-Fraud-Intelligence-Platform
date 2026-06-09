import time, json, random, uuid, logging, os
from kafka import KafkaProducer
from faker import Faker
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
fake = Faker('en_IN')
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
producer = None
while producer is None:
    try:
        producer = KafkaProducer(bootstrap_servers=[KAFKA_BROKER], value_serializer=lambda x: json.dumps(x).encode('utf-8'))
    except:
        time.sleep(2)
def main():
    while True:
        transaction = {
            "transaction_id": f"TXN-{uuid.uuid4().hex[:8]}",
            "user_id": f"U{random.randint(1,100):04d}",
            "amount": round(random.uniform(50000, 200000), 2) if random.random() < 0.05 else round(random.uniform(10, 5000), 2),
            "location": "International" if random.random() < 0.05 else random.choice(["Delhi", "Mumbai", "Bangalore"]),
            "timestamp": int(time.time() * 1000)
        }
        producer.send("transaction-raw", value=transaction)
        time.sleep(1.0 / int(os.getenv("TPS", "100")))
if __name__ == "__main__":
    main()
