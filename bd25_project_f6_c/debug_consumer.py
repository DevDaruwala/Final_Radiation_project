# debug_consumer.py

from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'processed-data',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='debug-group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("🔍 Listening to 'processed-data' Kafka topic...\n")

for message in consumer:
    enriched = message.value
    value = enriched["clean_data"]
    alert = enriched["cmp_alert"]
    rolling = enriched["rolling_average"]

    print("📍",
          "Time:", value.get("Captured Time"),
          "| Lat:", value.get("Latitude"),
          "| Lon:", value.get("Longitude"),
          "| Value:", value.get("Value"),
          "| Unit:", value.get("Unit"),
          "| Continent:", value.get("continent"),
          "| Country:", value.get("country"),
          "| Alert:", alert.get("alert"),
          "| Avg:", rolling.get("average"),
          "| Global Avg:", rolling.get("global_average"))
