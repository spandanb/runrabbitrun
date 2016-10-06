import hdfs, os
import json


hclient = hdfs.InsecureClient('http://{}:50070'.format(os.environ['PUBLIC_DNS']))

model = {"name":"John", "age":25, "city":"PA"}

path = '/results/model2.json'

with hclient.write(path, encoding='utf-8', overwrite=True) as writer:
    json.dump(model, writer)

with hclient.read(path) as reader:
    content = reader.read()
    print content
