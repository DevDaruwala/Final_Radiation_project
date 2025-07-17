import json
import time
import reverse_geocoder as rg
import pycountry
from kafka import KafkaProducer
from uuid import uuid4  # For generating unique event IDs

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8'))

# Giving Contenent
def map_to_continent(lat, lon):
    lat = float(lat)
    lon = float(lon)
    
    if -35 <= lat <= 37 and -17 <= lon <= 51:
        return "Africa"
    elif 35 <= lat <= 71 and -10 <= lon <= 40:
        return "Europe"
    elif -55 <= lat <= 12 and -80 <= lon <= -35:
        return "South America"
    elif 15 <= lat <= 70 and -170 <= lon <= -50:
        return "North America"
    elif -47 <= lat <= -10 and 110 <= lon <= 180:
        return "Oceania"
    elif -34 <= lat <= 60 and 60 <= lon <= 150:
        return "Asia"
    elif lat <= -60:
        return "Antarctica"
    else:
        return "Unknown"

# Stream data from CSV file to Kafka topic
with open('Small_file.csv', 'r') as f:
    header = f.readline()
    columns = header.strip().split(',')

    for line in f:
        value = line.strip().split(',')
        data_dict = dict(zip(columns, value))

        # Generate a unique event_id for tracking across all jobs
        data_dict['event_id'] = str(uuid4())

        # Add continent
        lat = data_dict.get('Latitude')
        lon = data_dict.get('Longitude')
        continent = map_to_continent(lat, lon)
        data_dict['continent'] = continent

        # Add country and city using reverse geocoding
        try:
            geo_result = rg.search((float(lat), float(lon)))[0]
            country_code = geo_result['cc']
            country = pycountry.countries.get(alpha_2=country_code)
            data_dict['country'] = country.name if country else "Unknown"
            data_dict['city'] = geo_result['name']
        except Exception as e:
            data_dict['country'] = "Unknown"
            data_dict['city'] = "Unknown"

        # Send data to Kafka topic 'radiation-stream'
        key = continent
        producer.send('radiation-stream', key=key, value=data_dict)
        producer.flush()

        print(f"Sent to Kafka | Key: {continent} | Event ID: {data_dict['event_id']}")
        time.sleep(1)
