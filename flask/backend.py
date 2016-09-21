"""
Takes a current position query 
"""
from elastic_wrapper.elastic_wrapper import ElasticWrapper
from utils.utils import pprint
import pdb
import dateutil.parser as dateparser
import math

def get_nearby(ew, lat, lon, dist=100):
    dist_query0 = {
        "query": {
            "filtered": {
                "filter": {
                    "geo_distance": {
                        "distance": dist,
                        "location": {
                            "lat": lat,
                            "lon": lon
                        }
                    }
                }
            }
        },
        "sort": [
            {
              "_geo_distance": {
                "location": { 
                  "lat": lat,  
                  "lon": lon 
                },
                "order":         "asc",
                "unit":          "m", 
                "distance_type": "plane" 
              }
            }
          ]
    }

    #This returns upto 20 results with 100m of the (lat, lon)
    points = ew.es.search(index=ew.index, doc_type=ew.type, body=dist_query0, size=20)

    path_map = {}
    #Gets all paths that are close by (contains dups)
    for point in points['hits']['hits']:
        path_id = point['_source']['path_id']
        #If the point already exists, then new point is a worse match 
        #since points are sorted by distance from user
        if path_id not in path_map:
            query_string = 'path_id:"{}"'.format(path_id)
            #TODO: size = should only be as many elements as the number of user points
            #sort by _doc:asc does what I expected _id:asc to do
            result = ew.es.search(index=ew.index, doc_type=ew.type, 
                                  q=query_string, size=1000, sort="_doc:asc")
            #ref_point (the point closest point), other_points (other points including ref_point)
            path_map[path_id] = {'ref_point': point, 'other_points': result['hits']['hits']}

    #This serves no purpose right now
    #user_ids = [point['_source']['user_id'] for point in points['hits']['hits']]
    #user_ids = list(set(user_ids))
    
    return path_map

def distance(lat1, lon1, lat2, lon2):
    radius = 6371 # km

    dlat = math.radians(lat2-lat1)
    dlon = math.radians(lon2-lon1)
    a = math.sin(dlat/2) * math.sin(dlat/2) + math.cos(math.radians(lat1)) \
        * math.cos(math.radians(lat2)) * math.sin(dlon/2) * math.sin(dlon/2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    d = radius * c

    return d

def compare_users(ew, user_points):
    #get_nearby(ew)

    #The computed values (will have be missing the tail element since speed is pairwise)
    user_path = []
    #the last element is not considered     
    for i, point in enumerate(user_points[:-1]):
        nextpoint = user_points[i+1]
        dist = distance(point[0], point[1], nextpoint[0], nextpoint[1])
        tdiff = (dateparser.parse(nextpoint[2]) - dateparser.parse(point[2])).total_seconds()
        speed = dist/tdiff/3600.0
        user_path.append((point[0], point[1], point[2], speed))
    
    #The other paths are based on the last point
    other_paths = get_nearby(ew, user_path[-1][0], user_path[-1][1])

    other_users = {}
    for path_id, path in other_paths.items():
        #Find the ref_point in path
        for i, point in enumerate(path['other_points']):
            #ref_point found in path  
            if point["_id"] == path['ref_point']["_id"]:
                #TODO: the following may not always be valid, i.e. when the historical data is at a start
                res = path['other_points'][i-len(user_path)+1:i+1]
                print "path_id={}, user_id={}".format(res[0]["_source"]['path_id'], res[0]["_source"]['user_id'] )
                print "your  speed= {}".format([u[3] for u in user_path])
                print "their speed= {}".format([r['_source']['speed'] for r in res])
                


def simpath_test(ew):
    """
    Tests a similar path, taken from 000, 20081023025304, 0:2
    """
    points = [(39.984702,116.318417,"02:53:04"),
              (39.984683,116.31845,"02:53:12"), 
              (39.984686,116.318417,"02:53:17")]

    compare_users(ew, points)

def combpath_test():
    """
    Tests a path is the result of a combined path
    """



if __name__ == "__main__":
    ew = ElasticWrapper()
    simpath_test(ew)

    
