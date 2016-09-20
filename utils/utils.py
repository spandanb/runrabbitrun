import json
import uuid

def get_id():
    return uuid.uuid4().hex
 
def pprint(obj):
    print json.dumps(obj, sort_keys=True, indent=4, separators=(',', ': '))
