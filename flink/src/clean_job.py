import json
from src.flink_connect import kafka_streaming
from src.data_cleaning import clean_record
from pyflink.common import Types
from pyflink.datastream.connectors import FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema


def parse_and_clean(record):
    """
    Parses the record from JSON string to dict, then cleans it.
    Returns cleaned record or None if invalid.
    """
    try:
        record_dict = json.loads(record)
        cleaned = clean_record(record_dict)
        if cleaned is None:
            return None

        # Construct final message
        enriched_record = {
            "event_id": record_dict.get("event_id"),
            "country": cleaned.get("country") or cleaned.get("Country"),
            "timestamp": cleaned.get("Captured Time") or cleaned.get("Timestamp"),
            "source": "clean",
            "payload": cleaned}


        return json.dumps(enriched_record)
    except Exception as e:
        print("Error while parsing and cleaning:", e)
        return None


def clean_job():
    # Step 1: Connect to Kafka
    env, data_stream = kafka_streaming()

    # Step 2: Clean and enrich data
    cleaned_stream = (
        data_stream
        .map(parse_and_clean, output_type=Types.STRING())
        .filter(lambda x: x is not None)
    )

    # Step 3: Output to console for debugging
    cleaned_stream.print()

    # Step 4: Kafka Sink to topic: processed-data
    kafka_sink = FlinkKafkaProducer(
        topic='processed-data',
        serialization_schema=SimpleStringSchema(),
        producer_config={'bootstrap.servers': 'kafka:9092'}
    )
    cleaned_stream.add_sink(kafka_sink)

    # Step 5: Run the Flink job
    env.execute("clean_job")


if __name__ == "__main__":
    clean_job()
