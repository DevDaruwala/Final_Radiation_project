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

#extract the 'Capture time" in flink

def event_time_extractor(jason_str):
    try:
        data = json.loads(jason_str)
        cap_time = data.get('Captured Time')
        dt = datetime.strptime(cap_time, '%Y-%m-%d %H:%M:%S')
        return int(dt.timestamp() * 1000)
    except:
        return 0  

#kafka environment

def kafka_streaming():
    config = Configuration()
    config.set_string(
        "pipeline.jars",
        "file:///opt/flink/lib/flink-connector-kafka-1.17.1.jar;file:///opt/flink/lib/kafka-clients-3.3.2.jar"
    )
    env = StreamExecutionEnvironment.get_execution_environment(config)
    env.set_parallelism(1)

    #kafka sourcee
    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers('localhost:29092') \
        .set_topics('radiation-stream') \
        .set_group_id('radiation-stream') \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()
    
    #Watermark strategy
    Watermark_Strategy = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(90)) \
        .with_idleness(Duration.of_seconds(180)) \
        .with_timestamp_assigner(lambda x, _: event_time_extractor(x)) 
        
    # Create DataStream
    data_stream = env.from_source(source=kafka_source, watermark_strategy=Watermark_Strategy, source_name="KafkaSource")
    
    return env, data_stream