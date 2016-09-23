try:
    from pyspark import SparkConf, SparkContext
except ImportError:
    pass
import os, pdb, time, json, math
from snakebite.client import Client as sbclient
from dateutil import parser as dateparser
from elastic_wrapper.elastic_wrapper import ElasticWrapper
from myutils.utils import get_users, get_trajectories


"""
This module processes the trajectories, 
namely computes speed between two points
and writes the entire tuple to elastic.
"""

def process_pairwise(row):
    """
    Processes geo points from the juxtaposed RDD and returns 
    an array representing just the first element, and the computed values.
    Computes the pairwise speed and elevation change between p0, p1.
    p0, p1 are in PLT format.
    Arguments:
        row:- a juxtaposed row, i.e. (idx, [p0, p1])
    """
    p0, p1 = row[1]

    #The time diff between p0, p1
    tdiff = (dateparser.parse((p1[6])) - dateparser.parse((p0[6]))).total_seconds() #seconds

    #Compute speed
    #src: https://gist.github.com/rochacbruno/2883505
    lat0, lon0 = p0[0], p0[1]
    lat1, lon1 = p1[0], p1[1]
    R = 6371 #Radius of Earth

    dlat = math.radians(lat1-lat0)
    dlon = math.radians(lon1-lon0)
    a = math.sin(dlat/2) * math.sin(dlat/2) + math.cos(math.radians(lat0)) \
        * math.cos(math.radians(lat1)) * math.sin(dlon/2) * math.sin(dlon/2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    d = R * c

    #TODO: Not sure if it makes sense to set speed to 0 if dt = 0
    try:
        #The speed in (km/h)
        speed = 3600 * d/tdiff
        #The gradient (m/s)
        gradient = ((p1[3] - p0[3]) * 0.3048)/tdiff 
    except ZeroDivisionError:
        speed = 0.0
        gradient = 0.0

    #lat, lon, p0[7] is the index
    return (p0[0], p0[1], speed, gradient, p0[7])

def get_path_data(filepath):
    "Extracts path data from the filepath"
    path_arr = filepath.split("/")
    user_id = path_arr[3]
    path_id = path_arr[-1].split(".")[0]
    return user_id, path_id

def to_elastic(ew, spark, filepath):
    """
    Computes the speed and elevation and 
    writes the result to elastic
    """

    public_dns = os.environ["PUBLIC_DNS"]
    user_id, path_id = get_path_data(filepath)
    filepath = "hdfs://{}:9000{}".format(public_dns, filepath)

    #Drops the first 6 lines, the alt way is to tag with index and 
    #drop line if index >= 6
    #transforms floats
    #Filters invalid lat-lon values
    rdd = spark.textFile(filepath)\
               .filter(lambda line: line.count(',') == 6)\
               .map(lambda line: line.split(','))\
               .map(lambda arr: [float(arr[0]), float(arr[1]), 0, float(arr[3]), arr[4], arr[5], arr[6]])\
               .filter(lambda arr: arr[0] >= -90.0 and arr[0] <= 90.0 and arr[1] >= -180.00 and arr[1] <= 180.00)\
               .zipWithIndex()

    #FIX: seems like this would be very inefficient
    tailidx = rdd.count() - 1 
    #Drop the tail element 
    #append index into value array, and swap order of index and value
    head = rdd.filter(lambda item_idx: item_idx[1] != tailidx)\
                  .map(lambda item_idx: (item_idx[1], item_idx[0] + [item_idx[1]]) )\

    #drop head element
    #decrement idx by 1 and reverse order of idx, and value
    tail = rdd.filter(lambda item_idx: item_idx[1] != 0 )\
                 .map(lambda item_idx: (item_idx[1]-1, item_idx[0]))\
   
    juxtaposed = head.join(tail)

    computed = juxtaposed.map(process_pairwise)\
                         .map(lambda row: {
                                 "location": {
                                    "lat": row[0],
                                    "lon": row[1]
                                 },
                                 "speed": row[2],
                                 "gradient": row[3],
                                 "user_id": user_id,
                                 "path_id": path_id,
                                 "offset": row[4] })\
                          .collect()       
                         #.foreachPartition(ew.create_document_multi_id) #Doesn't work

    print ew.create_document_multi_id(computed)


def main():
    """
    Processes all the data and writes to elastic.
    """
   
    #Initialize objects 
    public_dns = os.environ["PUBLIC_DNS"]
    hdfs = sbclient(public_dns, 9000, use_trash=False)
    ew = ElasticWrapper()

    #Create a config object
    conf = (SparkConf()
             .setMaster("spark://" + public_dns + ":7077")
             .setAppName(__file__)
             .set("spark.executor.memory", "5g"))
    
    #Get spark context
    sc = SparkContext(conf = conf)
    #Quiet the log
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org"). setLevel( logger.Level.ERROR )
    logger.LogManager.getLogger("akka").setLevel( logger.Level.ERROR )

    #iterate over all trajectory files and write to elastic
    for user in get_users(hdfs):
        #iterate over trajectories of this user
        for trajectory in get_trajectories(hdfs, user):
            to_elastic(ew, sc, trajectory['path'])

def reset_elastic():
    """
    Drops and rebuilds `geoindex` index
    """
    ew = ElasticWrapper()
    
    #Create index if required
    if ew.geo_index_exists():
        print ew.delete_index()
        time.sleep(5)    
    
    print ew.create_geo_index()
    

if __name__ == "__main__":
   main() 
   #reset_elastic()
    
