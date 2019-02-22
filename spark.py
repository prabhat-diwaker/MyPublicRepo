import sys
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from uuid import uuid1
import json
sc = SparkContext(appName="PythonSparkStreamingKafka")
sc.setLogLevel("WARN")
ssc = StreamingContext(sc,2)
topic='first'
kafkaParams = {"metadata.broker.list": 'localhost:9093',"auto.offset.reset": "largest"}
kafkaStream = KafkaUtils.createDirectStream(ssc, [topic], kafkaParams)
kafkaStream.pprint()
ssc.start()
ssc.awaitTermination(180)
