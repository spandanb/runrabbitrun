#!/bin/bash

source ../config.sample.sh

#For batch
$SPARK_HOME/bin/spark-submit --executor-memory 7000M --driver-memory 7000M $1

#For streaming
#$SPARK_HOME/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka_2.10:1.6.1 --executor-memory 7000M --driver-memory 7000M $1

