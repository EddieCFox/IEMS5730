package Question2
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.graphx.GraphLoader

object C {
  def main(args: Array[String]) {

    // Create Spark Context and load file
    val sc = new SparkContext()
    val graph = GraphLoader.edgeListFile(sc,"hdfs://dicvmd2.ie.cuhk.edu.hk:8020/user/s1155160788/edge_list.txt")

    // Question C queries
    val pageRank4330 = graph.personalizedPageRank(4330, 0.000000001, 0.15)
    val pageRank5730 = graph.personalizedPageRank(5730, 0.000000001, 0.15)

    println("Subsection I:")
    println("Top 20 Pagerank vertices for Vertex 4330:")
    pageRank4330.vertices.sortBy(_._2, ascending = false).take(20).foreach(println)

    println("\nTop 20 Pagerank vertices for Vertex 5730:")
    pageRank5730.vertices.sortBy(_._2, ascending = false).take(20).foreach(println)

    println("Subsection II:")
    val top2000Array = pageRank5730.vertices.sortBy(_._2, ascending = false).take(2000)
    val top2000RDD = sc.parallelize(top2000Array)
    val newGraph = graph.outerJoinVertices(top2000RDD)((vid, _, rank) => rank.getOrElse(-1))
    val subgraph = newGraph.subgraph(vpred = (id, attr) => attr != -1)
    val subgraph_vertex_count = subgraph.vertices.count()
    val subgraph_edge_count = subgraph.edges.count()
    println(s"The subgraph has $subgraph_vertex_count vertices and $subgraph_edge_count edges")
    sc.stop()
  }
}