# Use the base Flink image 
FROM flink:1.18

USER root

# Install Python and pip
RUN apt-get update && \
    apt-get install -y python3 python3-pip curl && \
    ln -s /usr/bin/python3 /usr/bin/python

# Install PyFlink version compatible with Flink 1.18
RUN pip3 install apache-flink==1.18.1

# Copy JARs (Kafka connectors etc.)
COPY jars/*.jar /opt/flink/connectors/

# Copy only the Flink source code
COPY flink/src /app/flink/src

# Set working directory
WORKDIR /app/flink

# Make Python aware of the Flink source modules
ENV PYTHONPATH="${PYTHONPATH}:/app/flink/src"
