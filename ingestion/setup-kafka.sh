#!/bin/bash

source ../config.sample.sh

#Creates the topic running-data
#/usr/local/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 2 --partitions 1 --topic $KAFKA_TOPIC

#Creates the topic computed-vals
/usr/local/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 2 --partitions 1 --topic $KAFKA_TOPIC_RES
