try:
    from pyspark import SparkConf, SparkContext
except ImportError:
    pass
from kafka import KafkaProducer
import os
from snakebite.client import Client as sbclient 
from myutils.utils import get_users, get_trajectories
import random
from dateutil import parser as dateparser
import json
import pdb
import subprocess 

def cointoss(odds=0.95):
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

def produce(hdfs):
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
                    print res
                    producer.send(os.environ['KAFKA_TOPIC'], res)

def main():
    public_dns = os.environ['PUBLIC_DNS']
    sbite = sbclient(public_dns, 9000, use_trash=False)
    produce(sbite)

if __name__ == "__main__":
    main() 


