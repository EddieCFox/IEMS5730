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

# Create the subgraph from Question 1B
subGraph = graph.filterEdges("TIMESTAMP >= 10000").filterEdges("TIMESTAMP <= 50000").dropIsolatedVertices()

# Question 1C Queries
motif_1 = subGraph.find("(a)-[e1]->(b); (c)-[e2]->(b)")
filter_1 = motif_1.filter("a.id != c.id").filter("e1.TIMESTAMP <= e2.TIMESTAMP")
print "Number of times motif I occurred: %d" % filter_1.count()

motif_2 = subGraph.find("(a)-[e1]->(b); (b)-[e2]->(c)")
filter_2 = motif_2.filter("a.id != c.id").filter("e1.TIMESTAMP <= e2.TIMESTAMP")
print "Number of times motif II occurred: %d" % filter_2.count()

motif_3 = subGraph.find("(a)-[e1]->(b); (c)-[e2]->(d); (a)-[e3]->(d); (c)-[e4]->(b)")
filter_3 = motif_3.filter("a.id != c.id").filter("b.id != d.id")\
.filter("e1.TIMESTAMP <= e2.TIMESTAMP").filter("e2.TIMESTAMP <= e3.TIMESTAMP").filter("e3.TIMESTAMP <= e4.TIMESTAMP")
print "Number of times motif III occurred: %d" % filter_3.count()

motif_4 = subGraph.find("(a)-[e1]->(b); (c)-[e2]->(d); (a)-[e3]->(e); (c)-[e4]->(e)")
filter_4 = motif_4.filter("a.id != c.id").filter("b.id != d.id").filter("b.id != e.id").filter("d.id != e.id")\
.filter("e1.TIMESTAMP <= e2.TIMESTAMP").filter("e2.TIMESTAMP <= e3.TIMESTAMP").filter("e3.TIMESTAMP <= e4.TIMESTAMP")
print "Number of times motif IV occurred: %d" % filter_4.count()