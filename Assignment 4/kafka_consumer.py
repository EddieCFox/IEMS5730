from pyspark.streaming.kafka import KafkaUtils
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SQLContext
import sys
import json
import time
import pprint

if __name__ == '__main__':
  sc = SparkContext(appName="A4-Consumer") # Create spark context and set app name
  sc.setLogLevel("WARN") # set log level to warn

  ssc = StreamingContext(sc, 10) # Create streaming context with batch duration of 10 seconds
  ssc.checkpoint('./checkpoint') # Create checkpoint directory for fault tolerance.

  # Create Kafka stream (Streaming context, zookeeper, consumer group, {topic, number of partitions to consume per topic})
  kafkaStream = KafkaUtils.createStream(ssc, 'dicvmd7.ie.cuhk.edu.hk:2181', 's1155160788-consumers', {'1155160788-test': 1})

  words = kafkaStream.map(lambda x: x[1]).flatMap(lambda x: x.split(" ")).map(lambda x: x.encode("ascii", "ignore")) # Split tweets into words.

  # Filter the list of words to only include those with # as the first character and are more than just #, in other words, hashtags
  hashtags = words.filter(lambda hash: len(hash) > 2 and '#' == hash[0])
  countedHashtags = hashtags.countByValueAndWindow(300, 120) #count the number of occurences of each hashtag, over 5 minute period, slide every 2 minutes
  sortedHashtags = countedHashtags.transform((lambda x: x.sortBy(lambda x:( -x[1])))) # Sort hashtags by frequency in descending order.
  sortedHashtags.pprint(30) # Print the top 30 results from each slide interval

  ssc.start() # Start streaming context.
  ssc.awaitTermination() # Await termination