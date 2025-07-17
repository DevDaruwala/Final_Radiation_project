FROM flink:1.18

USER root

# Install Python, pip, netcat, and Kafka dependencies
RUN apt-get update && \
    apt-get install -y python3 netcat-openbsd python3-pip curl && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    pip3 install apache-flink==1.18.1 kafka-python && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Copy Flink Kafka connector JARs
COPY jars/*.jar /opt/flink/connectors/

# Copy Python source files
COPY src /app/src

# Copy Kafka wait script
COPY wait-for-kafka.sh /app/wait-for-kafka.sh
RUN chmod +x /app/wait-for-kafka.sh

# Set working directory and PYTHONPATH
WORKDIR /app
ENV PYTHONPATH="${PYTHONPATH}:/app"
