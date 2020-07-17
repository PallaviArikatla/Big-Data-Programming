import sys
import os

import findspark
findspark.init('D:/UMKC/PB/Project/spark-2.4.5-bin-hadoop2.7')

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext.getOrCreate()
ssc = StreamingContext(sc, 1)
lines = ssc.socketTextStream("localhost", 6074)

Eachwords = lines.flatMap(lambda line: line.split(" "))
KeyPair = Eachwords.map(lambda word: (word, 1))
ResultCount = KeyPair.reduceByKey(lambda x, y: x + y)
ResultCount.pprint()

ssc.start()
ssc.awaitTermination()