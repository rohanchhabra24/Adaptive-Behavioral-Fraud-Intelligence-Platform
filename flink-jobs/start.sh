#!/bin/bash
echo "Waiting 15 seconds for Kafka to fully boot..."
sleep 15
echo "Submitting Unified Flink AI Pipeline..."
python src/unified_pipeline.py &
wait
