
Data Sets
---------

The network data is located at: http://snap.stanford.edu/data/index.html#socnets

The walking data is derived from: https://raw.githubusercontent.com/cambridgegis/cambridgegis_data/master/Traffic/Traffic_Signals/TRAFFIC_Signals.geojson

Running a pyspark job:
* REPL: $SPARK_HOME/bin/pyspark --master spark://<<master-hostname>>:7077
* Batch: $SPARK_HOME/bin/spark-submit <<python file>>

Files
-----

* +-- _batch (files related to batch proc)
* +-- data (various data files)
* |   
* +-- _misc 
* |   +-- genwalk.py - generate walking data
* |   +-- socgraph.py - experimenting with social graph data


