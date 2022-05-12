package Question2
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.graphx.GraphLoader

object A {
  def main(args: Array[String]) {

    def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
      if (a._2 > b._2) a else b
    }

    // Create Spark Context and load file
    val sc = new SparkContext()
    val graph = GraphLoader.edgeListFile(sc,"hdfs://dicvmd2.ie.cuhk.edu.hk:8020/user/s1155160788/edge_list.txt")

    // Question A queries
    val vertices_count = graph.vertices.count()
    val edges_count = graph.edges.count()
    val maxInDegree = graph.inDegrees.reduce(max)
    val maxOutDegree = graph.outDegrees.reduce(max)

    println(s"Number of vertices: $vertices_count")
    println(s"Number of edges: $edges_count")
    println(s"Vertex with the largest in-degree: ${maxInDegree._1}, with an in-degree of ${maxInDegree._2}")
    println(s"Vertex with the largest out-degree: ${maxOutDegree._1}, with an out-degree of ${maxOutDegree._2}")

    sc.stop()
  }
}