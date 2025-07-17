!/bin/sh
echo "Waiting for Kafka to be ready at $1..."

while ! nc -z "$(echo $1 | cut -d: -f1)" "$(echo $1 | cut -d: -f2)"; do
  sleep 1
done

echo "Kafka is ready. Starting Flink job..."
