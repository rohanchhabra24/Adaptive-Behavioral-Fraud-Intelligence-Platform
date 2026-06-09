#!/bin/bash
echo "Waiting 15 seconds for Kafka to fully boot..."
sleep 15
echo "Submitting Flink Job 1: Filtering and Enrichment..."
python src/01_filtering_and_enrichment.py &
sleep 10
echo "Submitting Flink Job 2: Behavior and Velocity..."
python src/02_behavior_and_velocity.py &
sleep 10
echo "Submitting Flink Job 3: Risk Evaluation..."
python src/03_risk_evaluation.py &
wait
