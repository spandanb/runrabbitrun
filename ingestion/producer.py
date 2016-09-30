try:
    from pyspark import SparkConf, SparkContext
except ImportError:
    pass
from kafka import KafkaProducer, KafkaConsumer
import os
from snakebite.client import Client as sbclient 
from myutils.utils import get_users, get_trajectories, get_id
import random
from dateutil import parser as dateparser
import datetime
import json
import pdb
import subprocess 
from multiprocessing import Process, Queue

def cointoss(odds=0.5):
    "Higher odds mean more likely to succeed"
    return random.random() < odds

def perturb(line):
    """
    Perturb the lat, lon values
    """
    step = 0.0001
    arr = line.strip().split(",")
    #mutate lat, lon 
    lat = float(arr[0])
    lat += step * random.randint(-2, 4)
    
    lon = float(arr[1])
    lat += step * random.randint(-2, 4)

    return [lat, lon, 0, float(arr[3]), arr[4], arr[5], arr[6]]

def produce_untagged(hdfs):
    """
    Produces random user input values. 
    untagged ~ no user or path info on the points
    """
    producer = KafkaProducer(bootstrap_servers='localhost:9092')

    for user in get_users(hdfs):
        if cointoss(): continue
            
        for trajectory in get_trajectories(hdfs, user):
            if cointoss(): continue
            
            filepath = "hdfs://{}:9000{}".format(os.environ['PUBLIC_DNS'], trajectory['path'])
            filepath = trajectory['path']

            cat = subprocess.Popen(["hadoop", "fs", "-cat", filepath], stdout=subprocess.PIPE)
            for lineno, line in enumerate(cat.stdout):
                if lineno > 6 and cointoss():
                    res = json.dumps(perturb(line))
                    #print res
                    producer.send(os.environ['KAFKA_TOPIC'], res)

def dtime():
    """
    Returns a string of current datetime
    """
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")


def produce_tagged(hdfs, channel=None, continuous=True):
    """
    Produces trajectory values for a single user, tagged with path_id and user_id 
    Arguments:
        hdfs: hdfs client
        channel: (Queue) to communicate with receiver process
        continuous: keep running
    """
    producer = KafkaProducer(bootstrap_servers='localhost:9092')

    def produce(): 
        for user in get_users(hdfs):
            if cointoss(odds=0.5): continue
                
            for trajectory in get_trajectories(hdfs, user):
                if cointoss(odds=0.5): continue
                
                filepath = "hdfs://{}:9000{}".format(os.environ['PUBLIC_DNS'], trajectory['path'])
                filepath = trajectory['path']
    
                cat = subprocess.Popen(["hadoop", "fs", "-cat", filepath], stdout=subprocess.PIPE)
                offset = 0
                user_id = get_id()
                path_id = get_id()
                channel.put({"user_id": user_id, "path_id": path_id})
                for lineno, line in enumerate(cat.stdout):
                    if lineno > 6 and cointoss():
                        data = perturb(line) + [ offset, user_id, path_id, dtime()]
                        offset += 1
                        res = json.dumps(data)
                        print res
                        producer.send(os.environ['KAFKA_TOPIC'], res)
    
                if not continuous:
                    return {"path_id": path_id, "user_id": user_id}

    if continuous: 
       while 1:  produce()
    else:
        produce()

def produce_controlled(hdfs):
    
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    test = [ 
        [39.984702, 116.318417, 0, 492, 39744.1201851852, "2008-10-23", "02:53:04", 1, "000", "20081023025304", dtime() ], 
        [39.984683, 116.318450, 0, 492, 39744.1202546296, "2008-10-23", "02:53:10", 2, "000", "20081023025304", dtime() ],
        [39.984686, 116.318417, 0, 492, 39744.1203125000, "2008-10-23", "02:53:15", 3, "000", "20081023025304", dtime() ]]

    print "Sending"
    for point in test:
        to_send = json.dumps(point)
        print to_send
        producer.send(os.environ['KAFKA_TOPIC'], to_send)
        
    print "Receiving"
    topic = os.environ['KAFKA_TOPIC_RES']
    #consumer = KafkaConsumer(topic, enable_auto_commit=False, group_id=None)
    #consumer = KafkaConsumer(topic)
    #consumer.topics()
    #consumer.seek_to_beginning()
    for recvd in KafkaConsumer(topic):
        print recvd



def kreceiver(channel):
    """
    Waits for the response from the spark stream job
    """
    #identifier = channel.get() #Not used
    topic = os.environ['KAFKA_TOPIC_RES']
    
    #The following sets Kafka offset to beginning
    #consumer = KafkaConsumer(topic, enable_auto_commit=False, group_id=None)
    #consumer.topics()
    #consumer.seek_to_beginning()

    for recvd in KafkaConsumer(topic):
        print recvd
        #resp is of type ConsumerRecord
        #See: https://pypi.python.org/pypi/kafka-python
        
        #latency = (datetime.datetime.now() - datetime.datetime.strptime(resp.value, "%Y-%m-%d %H:%M:%S.%f")).total_seconds()
        #print "Latency is {} seconds".format(latency)


def main_inst_prod():
    "Instrumented Producer"
    public_dns = os.environ['PUBLIC_DNS']
    sbite = sbclient(public_dns, 9000, use_trash=False)
    channel = Queue()
    proc = Process(target=kreceiver, args=(channel, ))
    proc.start()

    produce_tagged(sbite, channel=channel)
    proc.join()

def main_simple_prod():
    "Simple Producer"
    public_dns = os.environ['PUBLIC_DNS']
    sbite = sbclient(public_dns, 9000, use_trash=False)
    #produce_untagged(sbite)
    produce_controlled(sbite)

if __name__ == "__main__":
    main_inst_prod() 
    #main_simple_prod() 


