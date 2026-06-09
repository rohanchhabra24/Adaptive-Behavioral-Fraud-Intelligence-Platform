import os, json, time, pandas as pd
from kafka import KafkaConsumer, KafkaProducer
from sklearn.ensemble import IsolationForest
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
model = IsolationForest(contamination=0.05, random_state=42)
model.fit(pd.DataFrame([{"amount": 500}, {"amount": 800}, {"amount": 100}]))
def main():
    consumer = KafkaConsumer("transaction-raw", bootstrap_servers=[KAFKA_BROKER], value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    producer = KafkaProducer(bootstrap_servers=[KAFKA_BROKER], value_serializer=lambda x: json.dumps(x).encode('utf-8'))
    for msg in consumer:
        df = pd.DataFrame([{"amount": msg.value.get("amount", 0)}])
        prediction = model.predict(df)[0]
        producer.send("ml-scored", value={"transaction_id": msg.value["transaction_id"], "is_anomaly": int(prediction) == -1})
if __name__ == "__main__":
    while True:
        try:
            main()
        except:
            time.sleep(5)
