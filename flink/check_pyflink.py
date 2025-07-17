from pyflink.datastream import StreamExecutionEnvironment

env = StreamExecutionEnvironment.get_execution_environment()
j_env = env._j_stream_execution_environment

version = j_env.getExecutionEnvironment().getClass().getPackage().getImplementationVersion()
print("Flink version:", version)
