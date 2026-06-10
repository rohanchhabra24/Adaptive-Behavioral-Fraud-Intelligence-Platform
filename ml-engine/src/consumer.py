import os, json, time, logging
from kafka import KafkaConsumer, KafkaProducer
from openai import OpenAI

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

if not OPENAI_API_KEY:
    logger.error("CRITICAL: OPENAI_API_KEY is not set! GenAI Agent cannot start.")

client = OpenAI(api_key=OPENAI_API_KEY)

def analyze_with_openai(suspect):
    if not OPENAI_API_KEY:
        return "OpenAI API Key missing. Flink alert passed through."
        
    try:
        prompt = f"""
        You are an expert Fraud Analyst. Apache Flink has flagged a transaction mathematically as suspicious.
        Transaction details: {json.dumps(suspect, indent=2)}
        
        Is this a confirmed account takeover or severe fraud? 
        Respond in 1-2 short sentences summarizing your expert judgment. Start your response with "GenAI Verdict:".
        """
        
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=100,
            temperature=0.2
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        logger.error(f"OpenAI API Error: {e}")
        return f"GenAI Analysis Failed: {str(e)}"

def main():
    consumer = KafkaConsumer(
        "flink-suspects",
        bootstrap_servers=[KAFKA_BROKER],
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='earliest',
        group_id='genai-analyst-group'
    )
    
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    
    logger.info("GenAI Agent is listening to flink-suspects...")
    
    for msg in consumer:
        suspect = msg.value
        logger.info(f"Received suspect from Flink: {suspect['transaction_id']}")
        
        if suspect.get('severity') in ['HIGH RISK', 'MEDIUM RISK']:
            # Call OpenAI for deep inspection
            genai_reasoning = analyze_with_openai(suspect)
            
            # Combine Flink math with GenAI reasoning
            final_alert = {
                "transaction_id": suspect["transaction_id"],
                "risk_score": suspect["risk_score"],
                "severity": suspect["severity"],
                "reason": f"Flink: {suspect['reason']} | {genai_reasoning}"
            }
            
            producer.send("fraud-alerts", value=final_alert)
            producer.flush()
            logger.info(f"Published GenAI verified alert to fraud-alerts: {final_alert['transaction_id']}")

if __name__ == "__main__":
    while True:
        try:
            main()
        except Exception as e:
            logger.error(f"Consumer loop crashed: {e}")
            time.sleep(5)
