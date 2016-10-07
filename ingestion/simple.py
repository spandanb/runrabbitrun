import os
from kafka import KafkaProducer, KafkaConsumer
import threading
import sys

def simple_consume():
    consumer = KafkaConsumer(os.environ['KAFKA_TOPIC_RES'], bootstrap_servers=os.environ['KAFKA_BROKERS'])
    for msg in consumer:
        print "RECEIVED {}".format(msg)

def simple_produce():
    producer = KafkaProducer(bootstrap_servers=os.environ['KAFKA_BROKERS'])
    while 1:
        uinput = raw_input(">>>")
        print "SENDING {}".format(uinput)
        producer.send(os.environ['KAFKA_TOPIC'], uinput)

if __name__ == "__main__":
    if len(sys.argv) == 1:
        threading.Thread(target=simple_consume).start()
        simple_produce()
    elif sys.argv[1] == 'p':
        simple_produce()
    elif sys.argv[1] == 'c':
        simple_consume()

