#!/bin/bash

#Run kafka
sudo /usr/local/kafka/bin/kafka-server-start.sh /usr/local/kafka/config/server.properties &

#Creates the topic running-data
/usr/local/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3 --topic running-data
