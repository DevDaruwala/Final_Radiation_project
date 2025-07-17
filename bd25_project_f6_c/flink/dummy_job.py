import json
from flink_connect import kafka_streaming
from data_cleaning import clean_record

def parse_and_clean(record):
    """
    Parses the record from JSON string to dict, then cleans it.
    Returns cleaned record or None if invalid.
    """
    try:
        #print("🔥 RAW record from Kafka:", record)  # Debug log
        record_dict = json.loads(record)
        cleaned = clean_record(record_dict)
        #print("✅ Cleaned record:", cleaned)        # Optional debug
        return cleaned
    except Exception as e:
        print("❌ Parse or clean error:", e)
        return None

def dummy_flink_job():
    # Step 1: Connect to Kafka and get the environment + data stream
    env, data_stream = kafka_streaming()

    # Step 2: Clean the data (parse JSON, filter out invalids)
    cleaned_stream = (
        data_stream
        .map(parse_and_clean)
        .filter(lambda x: x is not None)
    )

    # Step 3: Print the cleaned data as it streams
    cleaned_stream.print()

    # Step 4: Execute the Flink job
    env.execute("Dummy Flink Job to Test Cleaned Kafka Stream")

if __name__ == "__main__":
    dummy_flink_job()
