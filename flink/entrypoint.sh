#!/bin/bash

FLINK_HOME=/opt/flink

# Function to check if Flink JobManager is up
wait_for_jobmanager() {
  local jobmanager_url="http://localhost:8081"
  local max_attempts=30
  local attempt=1

  while [[ "$(curl -s -o /dev/null -w ''%{http_code}'' $jobmanager_url)" != "200" ]]; do
    if [ $attempt -ge $max_attempts ]; then
      echo "Flink JobManager did not start within expected time."
      exit 1
    fi
    echo "Waiting for Flink JobManager to start... Attempt $attempt/$max_attempts."
    attempt=$((attempt+1))
    sleep 10
  done

  echo "Flink JobManager is up and running."
}

# Start the Flink cluster (JobManager and TaskManager)
$FLINK_HOME/bin/start-cluster.sh

# Wait for the Flink JobManager to start
wait_for_jobmanager

# Submit the Flink job
$FLINK_HOME/bin/flink run -py /flink/alert_job.py

# Keep the container running to maintain the Flink services
tail -f /dev/null