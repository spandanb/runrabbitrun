from pyspark import SparkConf, SparkContext
import os


def wordcount():
    """Performs word count on the file in hdfs: user/test.txt
    and prints output"""

    #Create a config object
    conf = (SparkConf()
             .setMaster("local")
             .setAppName("My app")
             .set("spark.executor.memory", "1g"))
    
    #Get spark context
    sc = SparkContext(conf = conf)
   
    public_dns = os.environ["PUBLIC_DNS"]
    filepath = "hdfs://{}:9000/user/test.txt".format(public_dns)
    fptr = sc.textFile(filepath)
    counts = fptr.flatMap(lambda line: line.split(" "))\
                 .map(lambda word: (word, 1))\
                 .reduceByKey(lambda a, b: a + b)
    
    #Collect and print results
    res = counts.collect()
    for val in res:
        print val

if __name__ == "__main__":
    wordcount()
