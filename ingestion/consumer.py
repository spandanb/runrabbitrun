from kafka import KafkaConsumer
from kafka.errors import KafkaError
import os

def consume():
    consumer = KafkaConsumer(os.environ['KAFKA_TOPIC'])
    for msg in consumer:
        print (msg)

if __name__ == "__main__":
    consume()
