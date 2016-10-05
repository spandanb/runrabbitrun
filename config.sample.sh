#!/bin/bash

#Public DNS of Spark used for Sparks
export PUBLIC_DNS="ec2-52-42-199-53.us-west-2.compute.amazonaws.com"
#elastic nodes
export ELASTIC_NODES="ec2-54-71-2-160.us-west-2.compute.amazonaws.com,ec2-52-39-68-18.us-west-2.compute.amazonaws.com,ec2-52-24-218-41.us-west-2.compute.amazonaws.com"
#Kafka topic for writting raw data to
export KAFKA_TOPIC="running-data2"
#Kafka topic for writting results to
export KAFKA_TOPIC_RES="computed-vals2"
#This is `localhost` because it is only used by Kafka
export ZOOKEEPER_LOC="localhost:2181"
export KAFKA_BROKERS="54.71.26.172:9092,52.37.14.144:9092,54.70.142.196:9092"
export GKEY="AIzaSyCtmsUnOVoi8gG1z703wTa6hcQ74LNA0bI"

