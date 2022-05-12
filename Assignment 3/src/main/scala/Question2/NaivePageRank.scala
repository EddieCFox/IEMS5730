package Question2
import org.apache.spark.sql.SparkSession

object NaivePageRank {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession // Create SparkSession
      .builder
      .appName("NaivePageRank")
      .getOrCreate()
    val sc = spark.sparkContext // Create sparkContext
    val filePath = args(0).toString // Takes first argument and stores it as file path.
    val links = sc.textFile(filePath) // Read textfile from file path
                .map(line => line.split("\t")) // Read each line split with tab separation
                .map(parts => (parts(0).trim, parts(1).trim)) // Remove leading whitespace from each part of line
                .distinct() // Remove any duplicate neighbors / links
                .groupByKey() // Group by each URL / Node / Link.
                .cache() // Cache links because we refer to it so much
    var ranks = links.mapValues(v => 1.0) // Set initial rank for each key in value to 1

    for (i <- 1 to 10) {
      val contribs = links.join(ranks).flatMap{
        case (url, (links, rank)) =>
          links.map(dest => (dest, rank/links.size))
      }
      ranks = contribs.reduceByKey(_ + _)
        .mapValues(0.15 + 0.85 * _)
    }
    // Sort by pagerank in descending order and take the top 100
    val top100 = ranks.sortBy(_._2, false).take(100)
    top100.foreach(X=> println(s"${X._1}\t${X._2}"))
  }
}