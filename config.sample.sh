#!/bin/bash

#Public DNS of Spark used for Sparks
export PUBLIC_DNS="ec2-52-32-170-79.us-west-2.compute.amazonaws.com"
#Kafka topic for writting raw data to
export KAFKA_TOPIC="running-data"
#Kafka topic for writting results to
export KAFKA_TOPIC_RES="computed-vals"
export ZOOKEEPER_LOC="localhost:2181"
export KAFKA_BROKERS="ec2-52-32-170-79.us-west-2.compute.amazonaws.com:9092, ec2-52-39-193-214.us-west-2.compute.amazonaws.com:9092, ec2-54-70-8-225.us-west-2.compute.amazonaws.com:9092, ec2-52-42-89-132.us-west-2.compute.amazonaws.com:9092"
export GKEY="AIzaSyCtmsUnOVoi8gG1z703wTa6hcQ74LNA0bI"

