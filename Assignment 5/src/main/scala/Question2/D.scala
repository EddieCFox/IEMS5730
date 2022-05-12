package Question2
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.graphx.lib.LabelPropagation

object D {
  def main(args: Array[String]) {

    // Create Spark Context and load file
    val sc = new SparkContext()
    val graph = GraphLoader.edgeListFile(sc,"hdfs://dicvmd2.ie.cuhk.edu.hk:8020/user/s1155160788/edge_list.txt")

    // Question D queries
    val communityGraph = LabelPropagation.run(graph, 50)
    val community_count = communityGraph.vertices.map{ case(_, community) => community}.distinct.count()
    println(s"Number of communities: $community_count")
    println("Top 10 Largest communities: Format: (Community ID, Number of Vertices)")
    communityGraph.vertices.map(x => (x._2, 1)).reduceByKey(_ + _).sortBy(_._2, ascending = false).take(10).foreach(println)
    sc.stop()
  }
}