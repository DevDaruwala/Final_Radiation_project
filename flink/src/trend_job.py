import json
from collections import deque
from src.flink_connect import kafka_streaming
from src.data_cleaning import clean_record
from pyflink.common import Types
from pyflink.datastream.connectors import FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema


# In-memory state
trend = {}  
glbl_v = []  
MAX_LEN = 25


def detection(values):
    if len(values) < 2:
        return "no trend"
    trend_score = 0
    for i in range(1, len(values)):
        if values[i] > values[i - 1]:
            trend_score += 1
        elif values[i] < values[i - 1]:
            trend_score -= 1

    if trend_score > 0:
        return "upward"
    elif trend_score < 0:
        return "downward"
    else:
        return "no trend"


def analisis(value):
    try:
        country = value.get("country", "Unknown")
        val = float(value["Value"])

        if country not in trend:
            trend[country] = deque(maxlen=MAX_LEN)

        window = trend[country]
        window.append(val)

        country_trend = detection(list(window))

        glbl_v.append(val)
        if len(glbl_v) > MAX_LEN:
            glbl_v.pop(0)

        global_trend = detection(glbl_v)

        return {
            "country": country,
            "country_trend": country_trend,
            "country_data_points": len(window),
            "global_trend": global_trend,
            "global_data_points": len(glbl_v)
        }

    except Exception as e:
        return {"trend_error": str(e)}


def parse_and_analyze(record):
    try:
        parsed = json.loads(record)
        cleaned = clean_record(parsed)
        if cleaned is None:
            return None

        trend_info = analisis(cleaned)
        if trend_info is None:
            return None

        # Extend trend_info with core fields
        trend_info.update({
            "Device ID": cleaned.get("Device ID"),
            "Captured Time": cleaned.get("Captured Time"),
            "Uploaded Time": cleaned.get("Uploaded Time")
        })

        return json.dumps({
            "event_id": cleaned.get("event_id"),
            "country": trend_info.get("country") or cleaned.get("Country"),
            "timestamp": cleaned.get("Captured Time") or cleaned.get("Timestamp"),
            "source": "trend",
            "payload": trend_info})


    except Exception as e:
        print(f"Error in trend analysis: {e}")
        return None


def trend_job():
    env, data_stream = kafka_streaming()

    analyzed_stream = (
        data_stream
        .map(parse_and_analyze, output_type=Types.STRING())
        .filter(lambda x: x is not None)
    )

    analyzed_stream.print()

    kafka_sink = FlinkKafkaProducer(
        topic='processed-data',
        serialization_schema=SimpleStringSchema(),
        producer_config={
            'bootstrap.servers': 'kafka:9092'
        }
    )

    analyzed_stream.add_sink(kafka_sink)

    env.execute("trend_job")


if __name__ == "__main__":
    trend_job()
