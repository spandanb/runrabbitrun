from flask import Flask, request, jsonify
from kafka import KafkaProducer, KafkaConsumer
import os
import threading, json

app = Flask(__name__, static_url_path='/static')

class StateManager(object):

    def __init__(self):
        self.producer = KafkaProducer(bootstrap_servers=os.environ['KAFKA_BROKERS'])
        self.consumer = KafkaConsumer(os.environ['KAFKA_TOPIC_RES'], 
                                      bootstrap_servers=os.environ['KAFKA_BROKERS'])
        self.cache = []

    def async_consume(self):
        def consume(): 
            print "Waiting to Consume"
            for msg in self.consumer:
                cache.append(msg)
        threading.Thread(target=consume).start()


@app.route("/")
def main():
    return app.send_static_file("index2.html")

@app.route("/coords", methods=["POST"])
def query():

    body = request.values['body']
    
    for b in body: statemanager.producer.send(os.environ['KAFKA_TOPIC'], json.dumps(b)) 
    return jsonify({"response" : 200})

@app.route("/resp", methods=["POST"])
def results():
    print "/RESP"
    print statemanager.cache
    return jsonify({"response" : 200})
    

if __name__ == "__main__":
    statemanager = StateManager()
    statemanager.async_consume()
    
    #app.run(host="0.0.0.0", debug=True, port=8080)
    #app.run(host="0.0.0.0", port=8080)
