import json
from src.flink_connect import kafka_streaming
from pyflink.common import Types
from pyflink.datastream.connectors import FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from collections import deque

# Store rolling states per country
rolling_states = {}
global_sum = 0.0
global_count = 0

# Calculate both per-country and global rolling averages
def get_rolling_average(value):
    global global_sum, global_count
    try:
        country = value.get("country", "Unknown")
        val = float(value["Value"])

        # Initialize per-country state if not present
        if country not in rolling_states:
            rolling_states[country] = deque(maxlen=30)

        dq = rolling_states[country]
        dq.append(val)
        country_avg = sum(dq) / len(dq)
        country_count = len(dq)

        # Update global average
        global_sum += val
        global_count += 1
        global_avg = global_sum / global_count

        return {
            'country': country,
            'average': round(country_avg, 2),
            'count': country_count,
            'global_average': round(global_avg, 2)
        }

    except Exception as e:
        return {'rolling_error': str(e)}

# Flink streaming job for rolling average
def rolling_job():
    env, data_stream = kafka_streaming()

    # Parse and process each Kafka record
    def process(record):
        try:
            record_dict = json.loads(record)
            enriched_payload = get_rolling_average(record_dict)

            # Include sync fields for combining later
            enriched_payload.update({
                "Device ID": record_dict.get("Device ID"),
                "Captured Time": record_dict.get("Captured Time"),
                "Uploaded Time": record_dict.get("Uploaded Time")
            })

            return json.dumps({
                "event_id": record_dict.get("event_id"),
                "country": enriched_payload.get("country") or record_dict.get("country"),
                "timestamp": record_dict.get("Captured Time") or record_dict.get("timestamp"),
                "source": "rolling",
                "payload": enriched_payload})

        except Exception as e:
            print(f"Error in rolling process: {e}")
            return None

    # Process stream
    stream = (
        data_stream
        .map(process, output_type=Types.STRING())
        .filter(lambda x: x is not None)
    )

    stream.print()  # for debugging

    # Send results to Kafka topic 'processed-data'
    sink = FlinkKafkaProducer(
        topic='processed-data',
        serialization_schema=SimpleStringSchema(),
        producer_config={'bootstrap.servers': 'kafka:9092'}
    )
    stream.add_sink(sink)

    env.execute("rolling_job")

if __name__ == "__main__":
    rolling_job()
