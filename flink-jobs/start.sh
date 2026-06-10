#!/bin/bash
echo "Waiting for Flink JobManager to fully boot..."
while ! curl -s http://jobmanager:8081/config > /dev/null; do
  echo "JobManager not ready yet. Retrying in 5 seconds..."
  sleep 5
done
echo "Flink JobManager is UP! Submitting Unified Flink AI Pipeline..."
python src/unified_pipeline.py &
wait
