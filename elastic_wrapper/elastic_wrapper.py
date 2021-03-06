from elasticsearch import Elasticsearch, helpers as eshelpers
import pdb
import os
from myutils.utils import get_id, pprint
#See: https://elasticsearch-py.readthedocs.io/en/master/api.html


class ElasticWrapper():
    def __init__(self, index="geodata", type="point"):
        enodes = os.environ['ELASTIC_NODES'].split(",")
        self.index = index
        self.type = type
        self.es = Elasticsearch(enodes)

    def list_indices(self):
        return self.es.indices.get_aliases().keys()

    def geo_index_exists(self):
        "Returns True if `self.index` exist"
        return self.index in self.list_indices()

    def create_geo_index(self):
        "Create an index for my geodata"
        config = {
            "mappings": {
                self.type :   {
                    "dynamic":"strict",
                    "properties" : {
                        "path_id"  : { "type" : "string" },   #Computed by spark, i.e. the stitching together of points
                        "user_id"  : { "type" : "string" },
                        "location" : { "type" : "geo_point" },
                        "speed"    : { "type" : "string" },
                        "gradient": { "type" : "string" }
                    }
                }
            }
        }
        return self.es.indices.create(index=self.index, body=config)

    def get_mapping(self):
        return self.es.indices.get_mapping(index=self.index, doc_type=self.type)

    def delete_index(self):
        "Deletes one or more indices"
        return self.es.indices.delete(index=[self.index])
    
    def create_document(self, doc):
        return self.es.create(index=self.index, doc_type=self.type, body=doc, id=get_id())
    
    def create_document_id(self, doc):
        identifier = "{}-{}".format(get_id()[:10], doc["offset"].zfill(6))
        doc = {
            "location" : doc["location"],
            "speed"    : doc["speed"],
            "gradient" : doc["gradient"],
            "user_id"  : doc["user_id"],
            "path_id"  : doc["path_id"]
         }

        return self.es.create(index=self.index, doc_type=self.type, body=doc, id=identifier)

    def create_document_multi(self, docs):
        """
        Bulk indexes multiple documents. 
        docs is a list of document objects.
        """
        def add_meta_fields(doc):
            return {
                "_index": self.index,
                "_type":  self.type,
                "_id"  : get_id(),
                "_source": doc
            }
    
        docs = map(add_meta_fields, docs)
        return eshelpers.bulk(self.es, docs)

        
    def create_document_multi_id(self, docs):
        """
        Bulk indexes multiple documents. 
        docs is a list of document objects.
        Creates an ascending index
        """
        prefix = get_id()[:10]

        def add_meta_fields(doc):
            return {
                "_index": self.index,
                "_type":  self.type,
                "_id"  : "{}-{}".format(prefix, str(doc["offset"]).zfill(6)),
                "_source": {
                    "location" : doc["location"],
                    "speed"    : doc["speed"],
                    "gradient" : doc["gradient"],
                    "user_id"  : doc["user_id"],
                    "path_id"  : doc["path_id"]
                }
            }
   
        docs = map(add_meta_fields, docs)
        return eshelpers.bulk(self.es, docs)
    
    def search_document(self, query):
        return self.es.search(index=self.index, doc_type=self.type, body=query)
    
    def get_all(self):
        query = {"query" : {"match_all" : {}}}
        return self.es.search(index=self.index, doc_type=self.type, body=query)

if __name__ == "__main__":
    ew = ElasticWrapper()

    doc0 = {
        "location":
            { "lat": 40.713,
              "lon": -73.986 },
        "speed": "32.4",
        "gradient": "42",
        "user_id": "1233",
        "path_id": "e23x"
    }

    ##Create a single document
    #print ew.create_document(doc0)

    ##Create multiple documents
    #ew.create_document_multi([doc0])

    dist_query = {
      "query": {
        "filtered": {
          "filter": {
            "geo_distance": {
              "distance": "100",
              "location": {
                "lat": 39.984346,
                "lon": 116.302601
              }
            }
          }
        }
      }
    }

    #print ew.search_document(dist_query)
    #pprint(ew.es.search(index=ew.index, doc_type=ew.type, q="path_id:20081023025304"))

    #pprint(ew.get_mapping())

    #pprint(ew.get_all())

#    print ew.delete_index()
#    print ew.create_geo_index()
    
    print ew.create_geo_index()
