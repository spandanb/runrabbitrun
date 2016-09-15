from elasticsearch import Elasticsearch
import pdb, json
import uuid
#See: https://elasticsearch-py.readthedocs.io/en/master/api.html

#CONSTANTS
INDEX = "geodata"
TYPE = "pin"

#UTILITY FUNCTIONS
def get_id():
    return uuid.uuid4().hex

def pprint(obj):
    print json.dumps(obj, sort_keys=True, indent=4, separators=(',', ': '))

def list_indices(es):
    return es.indices.get_aliases().keys()

def create_geo_index(es):
    "Create an index for my geodata"
    config = {
        "mappings": {
            TYPE :   {
                "dynamic":"strict",
                "properties" : {
                    "location" : { "type" : "geo_point" }
                }
            }
        }
    }
    es.indices.create(index=INDEX, body=config)

def get_mapping(es):
    return es.indices.get_mapping(index=INDEX, doc_type=TYPE)

def delete_index(es, index):
    "Deletes one or more indices; index is a list"
    return es.indices.delete(index=index)

def create_document(es, doc):
    return es.create(index=INDEX, doc_type=TYPE, body=doc, id=get_id())

def search_document(es, query):
    return es.search(index=INDEX, doc_type=TYPE, body=query)

def get_all(es):
    query = {"query" : {"match_all" : {}}}
    return es.search(index=INDEX, doc_type=TYPE, body=query)

if __name__ == "__main__":
    es = Elasticsearch(
        [{'host':'localhost'}] 
    )
    #print list_indices(es)

    doc = {
        "location":
            { "lat": 40.713,
              "lon": -73.986 }
    }
    #print create_document(es, doc)

    dist_query = {
      "query": {
        "filtered": {
          "filter": {
            "geo_distance": {
              "distance": "400",
              "location": {
                "lat":  40.715,
                "lon": -73.988
              }
            }
          }
        }
      }
    }

    #create_geo_index(es)

    print search_document(es, dist_query)

    #print delete_index(es, [INDEX])

    #pprint(get_mapping(es))

    #pprint(get_all(es))
