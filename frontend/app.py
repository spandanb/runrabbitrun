import tornado.ioloop
import tornado.web
import tornado.httpserver
import os, json, sys, pdb, threading
from kafka import KafkaProducer, KafkaConsumer
import threading

class StateManager(object):
    producer = None
    consumer = None
    cache = [] 

    @classmethod
    def set_producer(cls, producer):
        cls.producer = producer

    @classmethod
    def get_producer(cls):
        return cls.producer

    @classmethod
    def set_consumer(cls, consumer):
        cls.consumer = consumer

    @classmethod
    def get_consumer(cls):
        return cls.consumer


def simple_consumer():
    consumer = KafkaConsumer(os.environ['KAFKA_TOPIC_RES'], bootstrap_servers=os.environ['KAFKA_BROKERS'])
    for msg in consumer:
        print msg

def update_cache():
    print "IN UPDATE_CACHE"
    consumer = KafkaConsumer(os.environ['KAFKA_TOPIC_RES'], bootstrap_servers=os.environ['KAFKA_BROKERS'])
    StateManager.set_consumer(consumer)

    for msg in consumer:
        print msg
        StateManager.cache.append(msg)

class Application(tornado.web.Application):
    def __init__(self):
        handlers = [
            (r"/", MainHandler),
            (r"/coords", UserInputHandler),
            (r"/resp", UserResponseHandler),
        ]
        settings = {
            "static_path": os.path.join(os.path.dirname(__file__), "static"),
            "template_path": os.path.join(os.path.dirname(__file__), "static"),
            "debug": True
        }

        tornado.web.Application.__init__(self, handlers, **settings)

class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.render('index.html', googlekey=os.environ["GKEY"])

class UserInputHandler(tornado.web.RequestHandler):
    def post(self):
        body = self.get_argument("body")
        body = json.loads(body)
       
        print "BODY IS"
        print body

        if not StateManager.get_producer():
            #Create Kafka producer
            producer = KafkaProducer(bootstrap_servers=os.environ['KAFKA_BROKERS'])
            StateManager.set_producer(producer)

        producer = StateManager.get_producer()
        for b in body: producer.send(os.environ['KAFKA_TOPIC'], json.dumps(b)) 
        
        #producer.send(os.environ['KAFKA_TOPIC'], json.dumps(body))

        print "DONE"
        self.write("200") 

class UserResponseHandler(tornado.web.RequestHandler):
    def post(self):
        user_id = self.get_argument("user_id")

        print "In UserResponseHandler"
        print user_id

        print StateManager.cache 
        self.write("200") 


def main(port):
    http_server = tornado.httpserver.HTTPServer(Application())
    http_server.listen(port)
    tornado.ioloop.IOLoop.current().start()        
    threading.Thread(target=update_cache).start()
    
if __name__ == "__main__":
    port = 8080# sys.argv[1]
    main(port)


    
