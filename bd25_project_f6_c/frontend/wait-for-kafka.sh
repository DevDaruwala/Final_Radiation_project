#!/bin/sh

KAFKA_HOSTPORT=$1
TOPIC_NAME=$2

echo "Waiting for Kafka at $KAFKA_HOSTPORT..."

# Wait for Kafka to be reachable
while ! nc -z $(echo $KAFKA_HOSTPORT | cut -d: -f1) $(echo $KAFKA_HOSTPORT | cut -d: -f2); do
  sleep 1
done

echo "Kafka is up."

# Wait for at least one message in the topic
echo "Waiting for messages on topic '$TOPIC_NAME'..."

has_message=0

while [ "$has_message" -eq 0 ]; do
  kafka-console-consumer.sh \
    --bootstrap-server "$KAFKA_HOSTPORT" \
    --topic "$TOPIC_NAME" \
    --timeout-ms 2000 \
    --max-messages 1 > /dev/null 2>&1

  if [ $? -eq 0 ]; then
    has_message=1
    echo "Message found on topic '$TOPIC_NAME'."
  else
    echo "⏳ No message yet. Retrying in 2s..."
    sleep 2
  fi
done

# Run the actual consumer app
exec "$@"
