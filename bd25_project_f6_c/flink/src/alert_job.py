#alert_job.py

import json
from src.flink_connect import kafka_streaming
from src.data_cleaning import clean_record
from pyflink.common import Types
from pyflink.datastream.connectors import FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema


# Define it directly here — no import needed
def get_cmp_alert(value):
    try:
        if value is None:
            return None

        cmp_value = float(value['Value'])
        time_value = value['Captured Time']
        country = value.get('country', 'Unknown')

        if cmp_value > 60:
            return {
                'alert': 'High Radiation Level',
                'value': cmp_value,
                'time': time_value,
                'country': country
            }
        elif cmp_value > 30:
            return {
                'alert': 'Moderate Radiation Level',
                'value': cmp_value,
                'time': time_value,
                'country': country
            }
        else:
            return {
                'alert': 'Low Radiation Level',
                'value': cmp_value,
                'time': time_value,
                'country': country
            }

    except Exception as e:
        print(f"Error processing value: {e}")
        return None


def parse_and_classify_alert(record):
    try:
        parsed = json.loads(record)
        cleaned = clean_record(parsed)
        if cleaned is None:
            return None

        alert_info = get_cmp_alert(cleaned)
        if alert_info is None:
            return None

        # Add required fields
        alert_info.update({
            "Device ID": cleaned.get("Device ID"),
            "Captured Time": cleaned.get("Captured Time"),
            "Uploaded Time": cleaned.get("Uploaded Time")
        })

        return json.dumps({
            "event_id": cleaned.get("event_id"),
            "country": alert_info.get("country") or cleaned.get("Country"),
            "timestamp": cleaned.get("Captured Time") or cleaned.get("Timestamp"),
            "source": "alert",
            "payload": alert_info})


    except Exception as e:
        print(f"Error in alert parsing/classification: {e}")
        return None


def alert_job():
    env, data_stream = kafka_streaming()

    alert_stream = (
        data_stream
        .map(parse_and_classify_alert, output_type=Types.STRING())
        .filter(lambda x: x is not None)
    )

    alert_stream.print()

    kafka_sink = FlinkKafkaProducer(
        topic='processed-data',
        serialization_schema=SimpleStringSchema(),
        producer_config={'bootstrap.servers': 'kafka:9092'}
    )

    alert_stream.add_sink(kafka_sink)

    env.execute("alert_job")


if __name__ == "__main__":
    alert_job()
