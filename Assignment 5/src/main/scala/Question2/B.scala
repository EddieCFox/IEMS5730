package Question2
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.graphx.GraphLoader

object B {
  def main(args: Array[String]) {

    // Create Spark Context and load file
    val sc = new SparkContext()
    val graph = GraphLoader.edgeListFile(sc,"hdfs://dicvmd2.ie.cuhk.edu.hk:8020/user/s1155160788/edge_list.txt")

    // Question B queries
    val connectedGraph = graph.connectedComponents()
    val connected_count = connectedGraph.vertices.map{ case(_, cc) => cc}.distinct.count()
    println("Subsection I: Connected Components")
    println(s"Number of connected components in the citation network: $connected_count")
    println("The connected components are below.")
    println("Format: (Lowest vertex ID in connected component, Number of vertices in connected component)")
    connectedGraph.vertices.map(x => (x._2, x._2)).countByKey().foreach(println)

    println("Subsection II: Strongly Connected Components")
    val stronglyConnectedGraph = graph.stronglyConnectedComponents(5)
    val stronglyConnected_count = stronglyConnectedGraph.vertices.map(x => x._2).distinct.count
    println(s"Number of strongly connected components in the citation network: $stronglyConnected_count")

    sc.stop()
  }
}