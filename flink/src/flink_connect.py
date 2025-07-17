# flink_connect.py

from pyflink.common import Configuration
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors import KafkaSource
from pyflink.common.typeinfo import Types
from pyflink.common.time import Duration
from pyflink.datastream import ProcessFunction

import json
from datetime import datetime

# Now we will extract the 'Capture time" in flink as event time 

def event_time_extractor(jason_str):
    try:
        data_dict = json.loads(jason_str)
        capture_time = data_dict.get('Captured Time')
        dt = datetime.strptime(capture_time, '%Y-%m-%d %H:%M:%S')
        return int(dt.timestamp() * 1000)  # Convert to milliseconds
    except:
        return 0  # Default value in case of error

# Now we will make kafka environment

def kafka_streaming():
    # BEGIN: Added configuration for both required JARs
    config = Configuration()
    config.set_string(
        "pipeline.jars",
       # "file:///opt/flink/connectors/flink-connector-kafka-1.17.1.jar"
        "file:///opt/flink/connectors/flink-connector-kafka-1.17.1.jar;file:///opt/flink/connectors/kafka-clients-3.4.0.jar"
    )
    env = StreamExecutionEnvironment.get_execution_environment(config)
    # --- END: Added configuration for Kafka connector and Kafka client JARs ---
    env.set_parallelism(1)

    #kafka source
    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers('kafka:9092') \
        .set_topics('radiation-stream') \
        .set_group_id('radiation-stream') \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()
    
    # Watermark strategy
    Watermark_Strategy = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(90)) \
        .with_idleness(Duration.of_seconds(180)) \
        .with_timestamp_assigner(lambda x, _: event_time_extractor(x)) 
        
    # Create a DataStream from the Kafka source
    data_stream = env.from_source(source=kafka_source, watermark_strategy=Watermark_Strategy, source_name="KafkaSource")
    
    return env, data_stream

