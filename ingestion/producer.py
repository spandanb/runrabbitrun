from kafka import KafkaProducer
import os


def produce():
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    for _ in range(100):
        producer.send(os.environ['KAFKA_TOPIC'], b'some_message_bytes')


if __name__ == "__main__":
    produce()

