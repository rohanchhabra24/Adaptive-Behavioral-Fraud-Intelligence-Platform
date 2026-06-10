#!/bin/bash
echo "Waiting for Flink JobManager to fully boot..."
while ! wget -qO- http://jobmanager:8081/config > /dev/null; do
  echo "JobManager not ready yet. Retrying in 5 seconds..."
  sleep 5
done
echo "Flink JobManager is UP! Submitting Unified Flink AI Pipeline..."
flink run -m jobmanager:8081 -d -py src/unified_pipeline.py
