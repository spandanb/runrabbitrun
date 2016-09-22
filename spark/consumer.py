try:
    from pyspark import SparkConf, SparkContext
except ImportError:
    pass
from kafka import KafkaConsumer
from kafka.errors import KafkaError

from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import os, pdb, json
from myutils.utils import compute_distance 
import dateutil.parser as dateparser

#offsetRanges = []
#
#def storeOffsetRanges(rdd):
#    global offsetRanges
#    offsetRanges = rdd.offsetRanges()
#    return rdd
#
#def printOffsetRanges(rdd):
#    print type(rdd)
#    print dir(rdd)
#    for o in offsetRanges:
#        print "%s %s %s %s" % (o.topic, o.partition, o.fromOffset, o.untilOffset)

def _pairwise_compute(row):
    p0, p1 = row
    dist = compute_distance(p0[0], p0[1], p1[0], p1[1])
    tdiff = dateparser(p1[5]) - dateparser(p0[5]).total_seconds()
    #speed (km/h)
    speed = 3600 * dist/tdiff 
    #gradient m/s
    gradient = (p[1] - p[0])*0.3048/tdiff

    return (p0[0], p0[1], speed, gradient)

def pairwise_compute(rdd):
    """
    Performs speed, gradient computation
    """
    rdd = rdd.zipWithIndex()
    tailidx = rdd.count() - 1
    head = rdd.filter(lambda pair: pair[1] != tailidx)\
              .map(lambda pair: pair[0])
    tail = rdd.filter(lambda pair: pair[1] != 0)\
              .map(lambda pair: pair[0])
    juxtaposed = head.zip(tail)

    computed = juxtaposed.map(lambda row: _pairwise_compute(row))\
                         .collect()

    #Write this where you want
    with open("/home/ubuntu/foo", "w") as fptr:
        fptr.write(computed)
     
def simple_consume():
    consumer = KafkaConsumer(os.environ['KAFKA_TOPIC'])
    for msg in consumer:
        print (msg)

def consume():
    """
    Consume the user input data
    """
    public_dns = os.environ["PUBLIC_DNS"]
    #Create a config object
    conf = (SparkConf()
             .setMaster("spark://" + public_dns + ":7077")
             .setAppName(__file__)
             .set("spark.executor.memory", "2g"))
    
    #Get spark context
    sc = SparkContext(conf = conf)

    #quiet the logger
    #http://stackoverflow.com/questions/25193488
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org"). setLevel( logger.Level.ERROR )
    logger.LogManager.getLogger("akka").setLevel( logger.Level.ERROR )

    #The batch size is 3 seconds
    stream = StreamingContext(sc, 3)

    kafka_brokers = {"metadata.broker.list": os.environ["KAFKA_BROKERS"]}

    user_data = KafkaUtils.createDirectStream(stream, [os.environ['KAFKA_TOPIC']], kafka_brokers)
    user_data.map(lambda line: json.loads(line[1])).foreachRDD(pairwise_compute)

    stream.start()
    stream.awaitTermination()


if __name__ == "__main__":
    consume()
    #simple_consume()
