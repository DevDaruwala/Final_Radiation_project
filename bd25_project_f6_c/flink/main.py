# flink/main.py

from src.data_cleaning import clean_record
from src.rolling_avarage import get_rolling_average
from src.alert_job import get_cmp_alert
from src.flink_connect import kafka_streaming

import json
from pyflink.common import Types
from pyflink.datastream.connectors import KafkaSink, DeliveryGuarantee
from pyflink.common.serialization import SimpleStringSchema

def main_job():
    env, data_stream = kafka_streaming()

    cleaned_stream = (
        data_stream
        .map(lambda r: clean_record(json.loads(r)), output_type=Types.MAP(Types.STRING(), Types.STRING()))
        .filter(lambda x: x is not None)
    )

    def enrich_and_serialize(value):
        rolling = get_rolling_average(value)
        alert = get_cmp_alert(value)

        enriched = {
            "clean_data": value,
            "cmp_alert": alert,
            "rolling_average": rolling
        }

        return json.dumps(enriched)

    # ✅ Java-backed KafkaSink using Flink Kafka connector JARs
    kafka_sink = KafkaSink.builder() \
        .set_bootstrap_servers("kafka:9092") \
        .set_record_serializer(
            KafkaSink.simple_serializer(
                topic="processed-data",
                value_schema=SimpleStringSchema()
            )
        ) \
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
        .build()

    # ✅ Sink enriched data to Kafka topic
    cleaned_stream.map(enrich_and_serialize).sink_to(kafka_sink)

    print("✅ Flink job streaming enriched data to 'processed-data' topic...")
    env.execute("Flink Enrichment to Kafka")

if __name__ == "__main__":
    main_job()
