#!/bin/bash

source ../config.sample.sh

#$SPARK_HOME/bin/spark-submit $1 

$SPARK_HOME/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka_2.10:1.6.1 $1
