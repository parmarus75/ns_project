#!/bin/bash

# Download and Extract Kafka
wget http://apache.claz.org/kafka/2.1.0/kafka_2.11-2.1.0.tgz
tar -xzf kafka_2.11-2.1.0.tgz
mv kafka_2.11-2.1.0 kafka

# Set Kafka Home in PATH
echo 'PATH=$PATH:~/kafka/bin' >> ~/.bashrc
source ~/.bashrc

# Start Zookeeper & Kafka broker
cd kafka
bin/zookeeper-server-start.sh config/zookeeper.properties &


bin/kafka-server-start.sh config/server.properties &

# Create Kafka Topic
bin/kafka-topics.sh --create --zookeeper localhost:2181 \
--replication-factor 1 \
--partitions 1 \
--topic xml_topic


# Describe Topic
bin/kafka-topics.sh --zookeeper localhost:2181 --describe

# Run Kafka Producer for XML Topic
bin/kafka-console-producer.sh \
--broker-list localhost:9092 --topic xml_topic &

# type your sample kafka topic above when it prompt.


