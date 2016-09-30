import json
import uuid
import math

def get_id(integer=False):
    """
    Returns a random 128 bit hex string 
    """
    if integer:
        return uuid.uuid4().int
    else:
        return uuid.uuid4().hex
 
def pprint(obj):
    """
    Pretty prints an object
    """
    print json.dumps(obj, sort_keys=True, indent=4, separators=(',', ': '))
    
def get_users(hdfs, limit=-1):
    """
    Iterates over the users, i.e. the directory 
    object corresponding to each user
    Arguments:-
        hdfs: hdfs client
        limit: -1 (all), otherwise the max desired value
    """
    users = hdfs.ls(['/Geolife_Trajectories/Data/'])

    for i, u in enumerate(users):
        if i == limit: break
        yield u

def get_trajectories(hdfs, user, limit=-1):
    """
    Iterates over each file in the path; path should be a dir
    """
    userpath = user['path'] + "/Trajectory"
    trajectories = hdfs.ls([userpath])
    for i, t in enumerate(trajectories):
        if i == limit: break
        yield t

def compute_distance(lat0, lon0, lat1, lon1):
    R = 6371 #Radius of Earth

    dlat = math.radians(lat1-lat0)
    dlon = math.radians(lon1-lon0)
    a = math.sin(dlat/2) * math.sin(dlat/2) + math.cos(math.radians(lat0)) \
        * math.cos(math.radians(lat1)) * math.sin(dlon/2) * math.sin(dlon/2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    d = R * c
    return d
