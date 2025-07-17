import os
import json
import time
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
from collections import defaultdict
from datetime import datetime
from heapq import heappush, heappop
from itertools import count

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
SOURCE_TOPIC = os.getenv("SOURCE_TOPIC", "processed-data")
TARGET_TOPIC = os.getenv("TARGET_TOPIC", "final-data")

MAX_BUFFER = 10
FINAL_FIELD = [
    "country", "Device ID", "Captured Time", "average", "Value",
    "Unit", "Latitude", "Longitude", "global_average",
    "global_trend", "country_trend", "alert", "event_id"
]
UNWANTED_FIELDS = ["Uploaded Time", "timestamp", "city", "count"]

combined_messages = defaultdict(dict)
sent_event_ids = set()
message_heap = []
message_counter = count()

# Connect to Kafka broker
for attempt in range(10):
    try:
        consumer = KafkaConsumer(
            SOURCE_TOPIC,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="earliest",
            group_id="combine-group",
            enable_auto_commit=True
        )
        print(f"[INFO] Connected to Kafka at {BOOTSTRAP_SERVERS}")
        break
    except NoBrokersAvailable:
        print(f"[WARN] Kafka unavailable. Retrying ({attempt + 1}/10)...")
        time.sleep(3)
else:
    raise RuntimeError("Failed to connect to Kafka after multiple attempts.")

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print(f"[INFO] combine.py started. Listening to topic '{SOURCE_TOPIC}'")

for message in consumer:
    try:
        data = message.value
        payload = data.get("payload", {})
        event_id = data.get("event_id")

        if not event_id:
            print("[WARN] Skipping message without event_id.")
            continue
        if event_id in sent_event_ids:
            continue

        # Combining into one complete message
        buffer = combined_messages[event_id]
        buffer["event_id"] = event_id

        for key, value in data.items():
            if key != "payload" and str(value).lower() not in ["", "null", "none"]:
                buffer[key] = value

        for key, value in payload.items():
            if str(value).lower() not in ["", "null", "none"]:
                buffer[key] = value

        missing_fields = [
            f for f in FINAL_FIELD
            if f not in buffer or str(buffer[f]).strip().lower() in ["", "null", "none"]
        ]

        # Dropping incomplete messages 
        if missing_fields:
            print(f"[WAIT] Incomplete event_id={event_id}, missing: {missing_fields}")
            continue

        final_message = buffer.copy()
        for field in UNWANTED_FIELDS:
            final_message.pop(field, None)

        try:
            captured_time = datetime.fromisoformat(buffer["Captured Time"])
        except Exception as e:
            print(f"[ERROR] Invalid Captured Time for event_id={event_id}: {e}")
            print(f"[DEBUG] Buffer: {json.dumps(buffer)}")
            continue

        # Add complete message to heap, delay to preserve order
        heappush(message_heap, (captured_time, next(message_counter), event_id, final_message))
        combined_messages.pop(event_id, None)
        sent_event_ids.add(event_id)

        print(f"[INFO] Buffered complete event_id={event_id} with Captured Time={captured_time}")

        # Send if message is older than buffer threshold
        now = datetime.utcnow()
        while message_heap:
            oldest_time, _, eid, msg = message_heap[0]
            delay = (now - oldest_time).total_seconds()

            if delay >= MAX_BUFFER:
                heappop(message_heap)
                producer.send(TARGET_TOPIC, msg)
                print(f"[INFO] Sent event_id={eid} to topic '{TARGET_TOPIC}'")
            else:
                break

    except Exception as e:
        print(f"[ERROR] Failed to process message: {e}")
