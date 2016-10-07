import tornado.ioloop
import tornado.web
import tornado.httpserver
import os, json, sys, pdb
from kafka import KafkaProducer, KafkaConsumer
import threading
import hdfs

class StateManager(object):
    producer = None
    hdfsclient = None

    @classmethod
    def set_producer(cls, producer):
        cls.producer = producer

    @classmethod
    def get_producer(cls):
        return cls.producer

    @classmethod
    def set_hdfs(cls, hdfsclient):
        cls.hdfsclient = hdfsclient

    @classmethod
    def get_hdfs(cls):
        return cls.hdfsclient

#def simple_consumer():
#    consumer = KafkaConsumer(os.environ['KAFKA_TOPIC_RES'], bootstrap_servers=os.environ['KAFKA_BROKERS'])
#    for msg in consumer:
#        print msg
#
#def update_cache():
#    print "IN UPDATE_CACHE"
#    consumer = KafkaConsumer(os.environ['KAFKA_TOPIC_RES'], bootstrap_servers=os.environ['KAFKA_BROKERS'])
#    StateManager.set_consumer(consumer)
#
#    for msg in consumer:
#        print msg
#        StateManager.cache.append(msg)

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
        #TODO: Send in bulk
        for b in body: producer.send(os.environ['KAFKA_TOPIC'], json.dumps(b)) 
        #producer.send(os.environ['KAFKA_TOPIC'], json.dumps(body))

        self.write("200") 

class UserResponseHandler(tornado.web.RequestHandler):
    def _process_results(self, results):
        """
        Processes the results into format required by 
        high charts
        """
        #The matching speeds
        matches = []
        for path in results["matches"]:
            matches.append([])
            for point in path:
                matches[-1].append(point['_source']['speed'])
        #The user's path
        path = [point[2] for point in results["path"]]
        return {"matches": matches, "path": path}

    def post(self):
        user_id = self.get_argument("user_id")

        hdfsclient = StateManager.get_hdfs()
        if not hdfsclient:
            hdfsclient = hdfs.InsecureClient('http://{}:50070'.format(os.environ['PUBLIC_DNS']))
            StateManager.set_hdfs(hdfsclient)

        path = "/results/{}".format(user_id)
        if hdfsclient.status(path, strict=False):
            with hdfsclient.read(path) as reader:
                content = reader.read()
                content = json.loads(json.loads(content))
                content = self._process_results(content)
        else:
            content = {}
   
        self.write(content)

def main(port):
    http_server = tornado.httpserver.HTTPServer(Application())
    http_server.listen(port)
    tornado.ioloop.IOLoop.current().start()        
    
if __name__ == "__main__":
    port = sys.argv[1]
    main(port)


    
