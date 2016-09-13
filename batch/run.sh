#!/bin/bash

source ../config.sample.sh

$SPARK_HOME/bin/spark-submit spark-wordcount.py 
