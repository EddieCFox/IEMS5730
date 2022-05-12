package Question3
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.streaming._

object KafkaStreams {
  def main (args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("KafkaStreams")
      .getOrCreate()

    import spark.implicits._

    val consumer = spark // Read from Kafka topic
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "dicvmd7.ie.cuhk.edu.hk:6667")
      .option("subscribe", "1155160788-test")
      .load()
      .select($"value", $"timestamp")

    val lines = consumer.selectExpr("CAST(value AS STRING)", "timestamp").as[(String, Timestamp)]
    val words = lines.flatMap{
      case (value, stamp) => value.split(" ").map((_, stamp))
    }.toDF("value", "timestamp")

    val timeWords = words.withColumn("timestamp", from_unixtime(col("timestamp")))
    val stampWords = timeWords.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
    val hashtags = stampWords
        .filter(col("value").startsWith(("#")))
        .filter(length(col("value")) > 2)

    val windowDuration = "300 seconds"
    val slideDuration = "120 seconds"

    val countedHashtags = hashtags.groupBy(window($"timestamp", windowDuration, slideDuration), $"value")
      .count()
      .orderBy($"window".desc, $"count".desc)

    countedHashtags.writeStream
      .outputMode("complete")
      .format("console")
      .trigger(Trigger.ProcessingTime("120 seconds"))
      .option("path", "hdfs://dicvmd2.ie.cuhk.edu.hk:8020/user/s1155160788/output/")
      .option("checkpointLocation", "hdfs://dicvmd2.ie.cuhk.edu.hk:8020/user/s1155160788/checkpoint")
      .option("truncate", "false")
      .option("numRows", 30)
      .start()
      .awaitTermination()
  }
}