
from pyspark import SparkContext,HiveContext,SparkConf

sc= SparkContext()

file_rdd = sc.textFile("/a.txt")
a=file_rdd.map(lambda x : x.split(" ")).flatMap(lambda x : (x)).map(lambda x : (x,1))
a.reduceByKey(lambda x,y : x+y).saveAsTextFile("/my_output")



spark-submit job.py --deploy-mode cluster --master yarn --executor-cores 2 --driver-cores 2 --executor-memory 1G  --driver-memory 1G