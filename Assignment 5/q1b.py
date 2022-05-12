from pyspark.sql import SQLContext
from pyspark import SparkConf, SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from graphframes import *

sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)
spark = SparkSession(sc)

# Read files to create vertex and edge dataframes
vertices = spark.read.options(header='True', inferSchema='True', delimiter='\t').csv("hdfs://dicvmd2.ie.cuhk.edu.hk:8020/user/s1155160788/vertices.tsv")
edges_initial = spark.read.options(header='True', inferSchema='True', delimiter='\t').csv("hdfs://dicvmd2.ie.cuhk.edu.hk:8020/user/s1155160788/mooc_actions.tsv")

# Rename columns on edge dataframe to be compliant with GraphFrame requirements
edges = edges_initial.withColumnRenamed("USERID","src").withColumnRenamed("TARGETID","dst")

# Create the GraphFrame
graph = GraphFrame(vertices, edges)

# Question 1B Queries
subGraph = graph.filterEdges("TIMESTAMP >= 10000").filterEdges("TIMESTAMP <= 50000").dropIsolatedVertices()
print "Number of nodes in subgraph: %d" % subGraph.vertices.count()
print "Number of edges in subgraph: %d" % subGraph.edges.count()