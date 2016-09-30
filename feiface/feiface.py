from elastic_wrapper.elastic_wrapper import ElasticWrapper
from myutils.utils import pprint
import pdb
import dateutil.parser as dateparser
import math

def distance(lat1, lon1, lat2, lon2):
    "Returns the distance in km"

    radius = 6371 # km

    dlat = math.radians(lat2-lat1)
    dlon = math.radians(lon2-lon1)
    a = math.sin(dlat/2) * math.sin(dlat/2) + math.cos(math.radians(lat1)) \
        * math.cos(math.radians(lat2)) * math.sin(dlon/2) * math.sin(dlon/2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    d = radius * c

    return d

def nearby_points(ew, lat, lon, dist=10):
    "Return the nearby points"
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
    resp = ew.es.search(index=ew.index, doc_type=ew.type, body=dist_query0, size=10)
    return resp

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

    #This returns upto `size` results within `dist` of the (lat, lon), sorted by distance
    nearby_points = ew.es.search(index=ew.index, doc_type=ew.type, body=dist_query0, size=10)

    path_map = {}
    #Gets all paths that are close by (contains dups)
    for point in nearby_points['hits']['hits']:
        path_id = point['_source']['path_id']
        #If the point already exists, then new point is a worse match 
        #since points are sorted by distance from user
        if path_id not in path_map:
            query_string = 'path_id:"{}"'.format(path_id)
            #TODO: size = should only be as many elements as the number of user points
            #sort by _id, '_doc:asc' does what I expected '_id:asc' to do
            result = ew.es.search(index=ew.index, doc_type=ew.type, 
                                  q=query_string, size=10, sort="_doc:asc")
            #ref_point (the point closest point), other_points (other points including ref_point)
            path_map[path_id] = {'ref_point': point, 'other_points': result['hits']['hits']}

    #This serves no purpose right now
    #user_ids = [point['_source']['user_id'] for point in points['hits']['hits']]
    #user_ids = list(set(user_ids))
    
    return path_map

def get_user_path(user_points):
    """
    Takes the user_points and returns the user_path, i.e. with
    the speed and gradient. 
    Arguments:-
        user_points:- [(lat, lon, time)]
    """
    #The computed values (will have the tail element missing since speed is pairwise)
    user_path = []
    #the last element is not considered     
    for i, point in enumerate(user_points[:-1]):
        nextpoint = user_points[i+1]
        dist = distance(point[0], point[1], nextpoint[0], nextpoint[1])
        tdiff = (dateparser.parse(nextpoint[2]) - dateparser.parse(point[2])).total_seconds()
        speed = 3600 * dist/tdiff
        user_path.append((point[0], point[1], point[2], speed))
    return user_path

def compare_paths(ew, user_path):
    """
    Takes the user's trajectory and returns a list of other_path objects
    Arguments:- 
        ew: elastic_wrapper obj
        user_path: computed path with speed, gradient
    """
   
    if not user_path: 
        print "ERROR: user_path is empty"
        print user_path
        return []

    #The other paths are based on the last point
    nearby_paths = get_nearby(ew, user_path[-1][0], user_path[-1][1])

    other_paths = []
    for path_id, path in nearby_paths.items():
        #Find the ref_point in path
        for i, point in enumerate(path['other_points']):
            #ref_point found in path  
            if point["_id"] == path['ref_point']["_id"]:
                other_paths.append(path['other_points'][:i+1])
                
#                sidx = i-len(user_path)+1
#                eidx = i
#                if sidx > eidx: 
#                    print "ERROR: Incongruent Paths: (feiface.py)"
#                    continue #TODO: instead truncate the other result
#                    #FIX: This is going to give wonky results since we are trying to grab 
#                    #a subset of the array, larger than the array, e.g. the ref user started
#                    #after the active user. Perhaps, cut down the active user's path
#                prevpoints = path['other_points'][sidx:eidx+1]
#
#                other_path = {'path_id': prevpoints[0]["_source"]['path_id'], 
#                              'user_id': prevpoints[0]["_source"]['user_id'],
#                              'user_speed': [u[3] for u in user_path], 
#                              'their_speed': [p['_source']['speed'] for p in prevpoints]}
#                
#                other_paths.append(other_path)

    #Do something with this now
    return other_paths
                
def simpath_test(ew):
    """
    Tests a similar path, taken from 000, 20081023025304, 0:2
    """
    points = [(39.984702,116.318417,"02:53:04"),
              (39.984683,116.31845,"02:53:12"), 
              (39.984686,116.318417,"02:53:17")]

    results = compare_paths(ew, get_user_path(points))
    pdb.set_trace()
    return results

def other_test(ew):
    return get_nearby(ew, 39.984683, 116.31845)



if __name__ == "__main__":
    ew = ElasticWrapper()
    #pprint(simpath_test(ew))
    pprint(other_test(ew))

    
