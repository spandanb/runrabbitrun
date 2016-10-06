from __future__ import print_function
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.errors import KafkaError
import os, pdb, json
import dateutil.parser as dateparser
from feiface import feiface
from elastic_wrapper.elastic_wrapper import ElasticWrapper
import math
import hdfs
try:
    from pyspark import SparkConf, SparkContext
    from pyspark.streaming import StreamingContext
    from pyspark.streaming.kafka import KafkaUtils
except ImportError:
    pass

#Inlined from myutils.utils
def _distance(lat0, lon0, lat1, lon1):
    "Computes (km) distance between (lat0, lon0)->(lat1, lon1)"
    R = 6371 #Radius of Earth

    dlat = math.radians(lat1-lat0)
    dlon = math.radians(lon1-lon0)
    a = math.sin(dlat/2) * math.sin(dlat/2) + math.cos(math.radians(lat0)) \
        * math.cos(math.radians(lat1)) * math.sin(dlon/2) * math.sin(dlon/2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    d = R * c
    return d


def _speed_gradient(row):
    """
    Computes the speed and gradient
    Argument:
        row: (idx, [p0, p1]), 
            where p0, p1 are row objects
    """
    idx, pair = row
    p0, p1 = pair

    dist = _distance(p0[0], p0[1], p1[0], p1[1])
    tdiff = (dateparser.parse(p1[6]) - dateparser.parse(p0[6])).total_seconds()
    if tdiff != 0:
        #speed (km/h)
        speed = 3600 * dist/tdiff 
        #gradient m/s
        gradient = (p1[3] - p0[3])*0.3048/tdiff
    else:
        speed = 0
        gradient = 0

    if len(p0) == 7:
        #lat, lon, speed, gradient
        return (p0[0], p0[1], speed, gradient)
    else:
        #lat, lon, speed, gradient, offset, user_id, path_id, original datetime
        return [p0[0], p0[1], speed, gradient] + p0[7:] 
        #return (p0[0], p0[1], speed, gradient, p0[7], p0[8], p0[9], p0[10])

def pairwise_compute(rdd):
    """
    Performs speed, gradient computation.
    Juxtaposes each of the rows of the rdd.
    """
    #row -> (row, idx)
    rdd = rdd.zipWithIndex()
    tailidx = rdd.count() - 1
    head = rdd.filter(lambda pair: pair[1] != tailidx)\
              .map(lambda pair: (pair[1], pair[0]))
    tail = rdd.filter(lambda pair: pair[1] != 0)\
              .map(lambda pair: (pair[1]-1, pair[0]))
    
    #join and compute speed, gradient
    computed = head.join(tail)\
                   .map(_speed_gradient)
    return computed


def fetch_and_write(rdd):
    """
    Fetch the results and writes them to Kafka
    """
    producer = KafkaProducer(bootstrap_servers=os.environ['KAFKA_BROKERS'])

    #elastic wrapper
    ew = ElasticWrapper()
    point = rdd.take(1)
    print("RESULT IS")
    if point: 
        point = point[0]
        results = feiface.nearby_points(ew, point[0], point[1])
        #results = feiface.compare_paths(ew, rdd.collect())
        print(results)
        producer.send(os.environ["KAFKA_TOPIC_RES"], json.dumps(results))


def _get_subpath(ref, path):
    """
    Get subpath of path that corresponds to
    all points previous to ref
    """
    for i, point in enumerate(path):
        if ref['_id'] == point['_id']:
            return path[:i+1]
    return []


def fetch_and_pipe(rdd):
    """
    Fetch the results from Kafka and pipe them
    through Kakfka
    Arguments:
        rdd: the computed rdd, i.e. the output from pairwise_compute
    """
    try:
        #The tail element
        tail = rdd.zipWithIndex()\
                  .sortBy(lambda row: row[1], ascending=False)\
                  .map(lambda row: row[0])\
                  .first()
    except ValueError as err:
        if 'RDD is empty' != err.message:
            print("ValueError Raised: {}: {}".format(err.args, err.message))
        return 

    ew = ElasticWrapper() #See if this can be cached
    hclient = hdfs.InsecureClient('http://{}:50070'.format(os.environ['PUBLIC_DNS']))

    if tail:
        matching_paths = []
        #Get nearby points
        nearby_points = feiface.nearby_points(ew, tail[0], tail[1])
        #TODO: multiple nearby points may belong to the same path; filter
        for point in nearby_points['hits']['hits']:
            #get path_id and get all points in path
            path_id = point['_source']['path_id']

            #get all points in the path
            query_string = 'path_id:"{}"'.format(path_id)
            parent = ew.es.search(index=ew.index, doc_type=ew.type, 
                                  q=query_string, size=100, sort="_uid:asc")
            #get the matching subpath                      
            subpath = _get_subpath(point, parent['hits']['hits'])
            if subpath:
                matching_paths.append(subpath)
        
        #TODO: remove subset of path where distance > 10m
        user_id = tail[-1]
        #results = json.dumps({"user_id": user_id,  "matches": matching_paths}) #KAFKA
        results = json.dumps(matching_paths)  #HDFS
        print("Results is {}".format(results))
        
        #Write to Kafka output stream
        #TODO: FIX THIS
        #producer = KafkaProducer(bootstrap_servers=os.environ['KAFKA_BROKERS'])
        #producer.send(os.environ["KAFKA_TOPIC_RES"], results) 
        
        #write to file on hdfs
        with hclient.write('/results/' + user_id, encoding='utf-8', overwrite=True) as writer:
            json.dump(results, writer)




def simple_consume():
    #https://github.com/dpkp/kafka-python/issues/601
    topic = os.environ['KAFKA_TOPIC']
    #Sets the offset to 0
    #consumer = KafkaConsumer(topic, enable_auto_commit=False, group_id=None)
    #consumer.topics()
    #consumer.seek_to_beginning()
    
    consumer = KafkaConsumer(topic, bootstrap_servers=os.environ['KAFKA_BROKERS'])

    for msg in consumer:
        print (msg)

def consume_untagged(user_data, stream):
    """
    Consume the user input data
    Arguments:- 
        user_data: DStream representing incoming user data
        stream: spark streaming context
    """
    user_data.map(lambda line: json.loads(line[1]))\
             .transform(pairwise_compute)\
             .foreachRDD(print_results)

    stream.start()
    stream.awaitTermination()

def consume_tagged(user_data, stream):
    """
    user_data [0:6], field 7,8,9- offset, user_id, path_id
    """

    #Sanity test- should print something
    #user_data.foreachRDD(lambda rdd: print(rdd.take(1)))

    user_data.map(lambda line: json.loads(line[1]))\
             .transform(pairwise_compute)\
             .foreachRDD(fetch_and_pipe)

    stream.start()
    stream.awaitTermination()

def main():
    public_dns = os.environ["PUBLIC_DNS"]
    #Create a config object
    conf = (SparkConf()
             .setMaster("spark://" + public_dns + ":7077")
             .setAppName(__file__))
    
    #Get spark context
    sc = SparkContext(conf = conf)

    #quiet the logger
    #http://stackoverflow.com/questions/25193488
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org"). setLevel( logger.Level.ERROR )
    logger.LogManager.getLogger("akka").setLevel( logger.Level.ERROR )

    #The batch size is 3 seconds
    stream = StreamingContext(sc, 3)

    kafka_brokers = {"metadata.broker.list": os.environ['KAFKA_BROKERS']}

    user_data = KafkaUtils.createDirectStream(stream, 
                                              [os.environ['KAFKA_TOPIC']], 
                                              kafka_brokers)

    #consume_untagged(user_data, stream)
    consume_tagged(user_data, stream)

if __name__ == "__main__":
    main()
    #simple_consume()
