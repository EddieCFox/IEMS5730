package Question2
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.graphx.lib.LabelPropagation

object E {
  def main(args: Array[String]) {

    // Create Spark Context and load file
    val sc = new SparkContext()
    val graph = GraphLoader.edgeListFile(sc,"hdfs://dicvmd2.ie.cuhk.edu.hk:8020/user/s1155160788/dag_edge_list.txt")

    // Question E queries
    val initialGraph = graph.mapVertices((_,_) => 0)
    println("Top 20 nodes with largest distance from root. Format: (Vertex ID, Distance from Root)")
    val distanceGraph = initialGraph.pregel(initialMsg = 0)(
      (id,distance,newDistance) => math.max(distance,newDistance), // Vertex program
      (triplet)=> { // Send Message
        if(triplet.srcAttr+1 > triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr+1))
        } else {
          Iterator.empty
        }
      },
      (a:Int,b:Int) => math.max(a,b) // Merge message
    )
    distanceGraph.vertices.sortBy(_._2, ascending = false).take(20).foreach(println)
    sc.stop()
  }
}