from pyspark import SparkConf, SparkContext
import os
from snakebite.client import Client as sbclient

def get_file():
    """
    Connects to HDFS and returns a .plt file
    See: https://snakebite.readthedocs.io/en/latest/
    Returns filepath
    """
    
    return "/geo_test.plt"


    #TODO: should be a generator that iterates over (user, *.plt file) 

    public_dns = os.environ["PUBLIC_DNS"]
    hdfs = sbclient(public_dns, 9000, use_trash=False)
    files = hdfs.ls(['/Geolife_Trajectories/Data/000/Trajectory/'])
    return next(files)['path']


def avg_lat():
    """Calculates the average latitude"""

    #Create a config object
    conf = (SparkConf()
             .setMaster("local")
             .setAppName("My app")
             .set("spark.executor.memory", "1g"))
    
    #Get spark context
    sc = SparkContext(conf = conf)
   
    public_dns = os.environ["PUBLIC_DNS"]
    filepath = "hdfs://{}:9000{}".format(public_dns, get_file())
    print "filepath is {}".format(filepath)

    fptr = sc.textFile(filepath)
    #Lines with actual data have 6 commas, filter all other lines
    res = fptr.filter(lambda line: line.count(',') == 6)\
              .map(lambda line: line.split(",")[0]) #split line and extract the first element
    
    count = res.count()
    total = res.reduce(lambda a,b: float(a) + float(b))

    print "Avg is {}".format(total/float(count))

if __name__ == "__main__":
    geodata()
