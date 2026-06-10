# Capstone Project: Real-Time Fraud Detection System for Digital Payments

## 1. Project Overview & Business Context

Digital payment platforms process millions of transactions per second. The key challenge they face is that fraudulent transactions happen in milliseconds. Traditional batch processing is far too slow to catch these anomalies; by the time a batch job runs overnight, the stolen funds have already been transferred out. Organizations require real-time stream processing systems to analyze transactions instantly, detect suspicious patterns, and trigger alerts before the money leaves the system.

This project delivers an end-to-end **Real-Time Fraud Detection System** built on a Hybrid Agentic Architecture. It combines the raw computational speed of **Apache Flink** with the advanced reasoning capabilities of a **Generative AI Agent (OpenAI)**, connected via **Apache Kafka** event streams.

### 1.1 Business Problems Solved
- **Millisecond Latency:** Catches fraud in real-time before transactions settle.
- **Volume Handling:** Processes thousands of transactions per second (TPS) without bottlenecking.
- **Contextual Intelligence:** Uses a GenAI agent to provide human-readable reasoning for *why* a transaction was blocked, drastically reducing manual investigation time for fraud analysts.

---

## 2. Solution Architecture at a Glance

The solution is divided into a high-speed mathematical layer (Flink) and a deep-reasoning intelligence layer (OpenAI), orchestrated by Kafka.

| Component | Technology | Outcome |
| :--- | :--- | :--- |
| **Data Ingestion** | Python, Apache Kafka | Simulates real-time digital payment transactions at up to 100+ TPS. |
| **Stream Processing** | Apache Flink (PyFlink SQL) | Computes real-time sliding/tumbling windows, velocity metrics, and behavioral scores. |
| **Fraud Intelligence** | OpenAI GPT-4o-mini | Acts as an autonomous agent to evaluate Flink's mathematical suspects and provide natural language reasoning. |
| **Output / API Layer** | FastAPI, WebSockets | Pushes live metrics and GenAI fraud alerts asynchronously. |
| **Dashboard** | React, Material UI | Provides a live streaming observatory for analysts to monitor the pipeline. |

---

## 3. Data Ingestion Layer

The system simulates a high-throughput stream of digital payment transactions using a custom Python Kafka Producer (`transaction-generator`).

### 3.1 Streaming Transaction Schema
Each event is generated as a JSON payload and pushed to the `transaction-raw` Kafka topic:
```json
{
  "transaction_id": "TXN-8f3a9b21",
  "user_id": "U0042",
  "amount": 45000.50,
  "location": "International",
  "timestamp": 1718041234000,
  "is_simulated_fraud": false
}
```

### 3.2 Event-Driven Control
The generator itself is controlled via an event-driven `system-control` Kafka topic. When an analyst clicks "Start Stream" on the UI, a signal is pushed to Kafka, instantly waking the generator to begin producing exactly 100 Transactions Per Second (TPS).

---

## 4. Stream Processing with Apache Flink

All data processing logic is encapsulated in `unified_pipeline.py`. Apache Flink was chosen over Spark Streaming because Flink provides **true native stream processing** (event-by-event) rather than micro-batching, ensuring absolute minimum latency.

### 4.1 Window-Based Analytics & Aggregations
Flink SQL uses complex windowing functions to build a real-time behavioral profile for every user:
- **Tumbling/Sliding Windows:** Calculates the `hourly_avg_spend` for each `user_id` using an `OVER` window spanning the last 1 hour.
- **Velocity Tracking:** Counts the number of transactions a user has made in the last 10 minutes (`velocity_count`).

### 4.2 Real-Time Fraud Logic
A transaction is evaluated against the user's live historical profile:
1. **Behavior Score:** `(Current Amount / Hourly Avg Spend) * 10`
2. **Velocity Penalty:** Adds `10.0` points for every transaction made in the last 10 minutes.
3. **Risk Score Calculation:** Combines behavior and velocity into a capped `risk_score` (max 100.0).

```sql
SELECT 
    transaction_id, 
    CASE 
        WHEN ((behavior_score * 3.0) + (velocity_count * 10.0)) > 100.0 THEN 100.0
        ELSE ((behavior_score * 3.0) + (velocity_count * 10.0))
    END AS risk_score
FROM behavior_scored
```
Any transaction with a score >= 50 is immediately pushed to the `flink-suspects` Kafka topic.

---

## 5. GenAI Fraud Intelligence (Output Layer)

Mathematical scores are fast but lack context. The `ml-engine` consumes from `flink-suspects` and acts as a **Generative AI Fraud Analyst**. 

### 5.1 LLM Synthesis
It feeds the mathematical metrics into OpenAI's API with a strict system prompt:
> *"You are an expert fraud analyst... Analyze the metrics and provide a 1-sentence explanation of why this was flagged."*

### 5.2 Real-Time Output & WebSockets
The AI's verdict is published to the `fraud-alerts` Kafka topic. The FastAPI backend consumes this topic and broadcasts the alerts via **WebSockets** directly to the React frontend. This creates a live, auto-updating dashboard where human analysts can watch fraud being blocked in real-time without ever refreshing the page.

---

## 6. Scalability & Fault Tolerance

The pipeline is designed for enterprise-grade scalability and resilience:
- **Kafka Partitioning:** Topics can be partitioned to distribute the streaming load across multiple Flink TaskManagers.
- **Topic-Pattern Discovery:** Flink is configured with `scan.topic-partition-discovery.interval`, allowing it to dynamically discover new Kafka topics without crashing if they are created dynamically.
- **Earliest-Offset Replay:** Both Flink and the ML Engine are configured with `auto_offset_reset='earliest'`. If a container crashes, it will automatically restart and re-process all historical messages it missed, guaranteeing **zero data loss**.
- **Dockerized Microservices:** Every component (Zookeeper, Kafka, Generator, Flink JobManager, Flink TaskManager, ML-Engine, FastAPI, React) runs in fully isolated, horizontally scalable Docker containers.

---

## 7. Setup & Execution Instructions

### Prerequisites
- Docker & Docker Compose
- OpenAI API Key

### Step 1: Environment Setup
Add your OpenAI API key to the `.env` file in the root directory:
```bash
OPENAI_API_KEY=sk-your-key-here
```

### Step 2: Build and Launch the Cluster
```bash
docker compose build --no-cache
docker compose up -d
```
*Note: Kafka takes ~15-20 seconds to fully initialize. Python services are configured with `restart: always` to automatically reconnect once Kafka is ready.*

### Step 3: Access the Live Dashboard
Open your browser and navigate to: **http://localhost:3000**

### Step 4: Run the Simulation
1. Go to the **Control** tab and click **Start Stream**.
2. Navigate to the **Kafka Stream** tab to watch the live TPS (Transactions Per Second) counter hit 100+.
3. Navigate to the **Intelligence** tab to watch the GenAI Fraud Analyst catch anomalous transactions and generate real-time verdicts.
