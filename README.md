# [Run Rabbit Run](http://runrabbitrun.xyz)

## Table of contents
1. [Introduction](README.md#introduction)
3. [Data Pipeline](README.md#data-pipeline)
2. [AWS Clusters](README.md#aws-clusters) 
4. [Performance](README.md#performance)
5. [Resources](README.md#resources)
6. [Files](README.md#files)

## Introduction 
[Back to Table of contents](README.md#table-of-contents)

[Run Rabbit Run](http://runrabbitrun.xyz) is a platform for monitoring your
relative speed compared to other users or your own past self, e.g. Mario Kart
ghost racer. The user in real time can ask, who has run on the same path as me
and how fast were they running? The idea is that gamifying the running experience
thus will allow user's to stay consistent towards their running goals and improve 
their performance over time.

![Alt text](misc/runrabbitrun_chart.png?raw=true "Relative Speeds")

## Data Pipeline
[Back to Table of contents](README.md#table-of-contents)

The pipeline for this looks as follows:

![Alt text](misc/runrabbitrun_pipeline.png?raw=true "Data Pipeline")

### Spark Streaming
Spark Streaming is used to compute the active users' speed and get the matching
results from the database.

### Spark Batch
Spark Batch is used to compute the historical speeds of users.

### HDFS
All the historical data is stored in HDFS.

### Elastic
Elastic is used to enable geospatial queries.

### Kafka 
Kafka is used as a buffer for user request and the results computed by Spark.

### Tornado
Tornado is the web server used to serve the web app. Tornado was chosen for
its ability to support a large number of connections.

## AWS Clusters
[Back to Table of contents](README.md#table-of-contents)
[Run Rabbit Run](http://runrabbitrun.xyz) runs on four clusters on AWS:
<ul>
<li>1 m3.large instance for Spark master (streaming and batch processing)</li>
<li>3 m4.xlarge instances for Spark workers and HDFS</li>
<li>3 m3.large instances for Kafka</li>
<li>3 r3.large instances for Elasticsearch</li>
<li>1 m4.xlarge node for Tornado</li>
</ul>
This amounts to a hourly running cost of $1.9.

## Performance
[Back to Table of contents](README.md#table-of-contents)

The System can handle 3000 events/seconds with a ~1 second latency.

## Resources 
[Back to Table of contents](README.md#table-of-contents)

### Website 
http://runrabbitrun.xyz
### Presentation
http://bit.do/runrabbitrun
### Video 
https://youtu.be/RnIgcWHt_yc

## Files 
[Back to Table of contents](README.md#table-of-contents)

* +-- data (various data files)
* +-- _elastic_wrapper
* |-- +-- elastic_wrapper.py - wrapper around elasticsearch-py 
* +-- _feiface
* |-- +-- feiface.py 
* +-- _spark (files related to spark stream and batch processing)
* |-- +-- process_trajectories.py - processes historic paths
* |-- +-- RT_process_trajectories.py - processes incoming events 


