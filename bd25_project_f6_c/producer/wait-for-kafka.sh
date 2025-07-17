#!/bin/sh

# Check Kafka host:port directly inside the script
KAFKA_HOST="kafka"
KAFKA_PORT=9092

echo "Waiting for Kafka at $KAFKA_HOST:$KAFKA_PORT..."

until nc -z $KAFKA_HOST $KAFKA_PORT; do
  echo "Kafka not yet available — retrying..."
  sleep 2
done

echo "Kafka is reachable, waiting 10 more seconds for full startup..."
sleep 10

exec "$@"
