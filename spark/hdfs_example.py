import hdfs, os
import json


model = {"name":"John", "age":25, "city":"PA"}
path = '/results/model2x.json'
hclient = hdfs.InsecureClient('http://{}:50070'.format(os.environ['PUBLIC_DNS']))

def read():
    if hclient.status(path, strict=False):
        with hclient.read(path) as reader:
            content = reader.read()
            print content
    else:
        print None


def write():
    with hclient.write(path, encoding='utf-8', overwrite=True) as writer:
        json.dump(model, writer)

if __name__ == "__main__":
    read()


