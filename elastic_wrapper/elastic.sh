#!/bin/bash

#script that does various elastic ops

ARG=$1

if [ $ARG -eq "0" ]; then
#Hello
curl 'localhost:9200'

elif [ $ARG -eq "1" ]; then
#List all indices
curl 'localhost:9200/_cat/indices?v'

elif [ $ARG -eq "2" ]; then
#Get mapping for an index
curl -XGET 'http://localhost:9200/geodata/_mapping/?pretty'

elif [ $ARG -eq "3" ]; then
#Add new index , and pretty print it
curl -XPUT 'localhost:9200/geodata?pretty' -d '
{
    "mappings": {
        "pin" :   {
            "dynamic":"strict",
            "properties" : {
                "location" : { "type" : "geo_point" }
            }
        }
    }
}'

elif [ $ARG -eq "4" ]; then
#Delete an index
curl -XDELETE 'localhost:9200/geodata?pretty'

elif [ $ARG -eq "5" ]; then
#Add a record- conforms with mapping
curl -XPUT 'localhost:9200/geodata/pin/3?pretty' -d '
{
    "location" : {
        "lat": 40.713,
        "lon": -73.986
     }
}'

elif [ $ARG -eq "7" ]; then
#Add a record- DOES NOT conform with mapping
curl -XPUT 'localhost:9200/geodata/pin/4?pretty' -d '
{
    "pin" : {
        "location" : {
            "lat": 40.712,
            "lon": -73.985
         }
    }
}'

elif [ $ARG -eq "8" ]; then
#Print all
curl -XPOST 'localhost:9200/geodata/_search?pretty=true&q=*:*'

elif [ $ARG -eq "6" ]; then
#First approach to geo query, 425 passes, 415 fails
curl -XPOST 'localhost:9200/geodata/_search?pretty' -d '
{
  "query": {
    "filtered": {
      "filter": {
        "geo_distance": {
          "distance": "415",
          "location": {
            "lat":  40.715,
            "lon": -73.988
          }
        }
      }
    }
  }
}'
else
    echo "other"
fi

