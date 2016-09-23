import tornado.ioloop
import tornado.web
import tornado.httpserver
import os, json


class Application(tornado.web.Application):
    def __init__(self):
        handlers = [
            (r"/", MainHandler),
            (r"/coords", UserInputHandler),
    
        ]
        settings = {
            "static_path": os.path.join(os.path.dirname(__file__), "static"),
            "template_path": os.path.join(os.path.dirname(__file__), "static"),
            "debug": True
        }
        tornado.web.Application.__init__(self, handlers, **settings)

class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.render('index.html')

class UserInputHandler(tornado.web.RequestHandler):
    def post(self):
        body = self.get_argument("body")
        body = json.loads(body)
        self.write("200") 
        
def main():
    http_server = tornado.httpserver.HTTPServer(Application())
    http_server.listen(9009)
    tornado.ioloop.IOLoop.current().start()        

if __name__ == "__main__":
    main()
