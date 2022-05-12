package Question3
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._ // import built in dataframe API functions

object Crime2013A {
  def main (args: Array[String]): Unit = {
    val spark = SparkSession // Create SparkSession
      .builder
      .appName("Crime2013")
      .getOrCreate()
    val filePath = args(0).toString // Takes first argument and stores it as file path.

    val crimesTable = spark.read.format("csv") // read CSV formatted file
      .option("sep", ",") // comma separated
      .option("inferSchema", "true") // infer schema
      .option("header", "true") // csv file has header
      .load(filePath) // load file corresponding to the first argument

    // Select only these 6 columns
    val focusedCrimesTable = crimesTable.select("CCN","REPORT_DAT", "OFFENSE", "METHOD", "END_DATE", "DISTRICT")
    val filteredCrimesTable = focusedCrimesTable.na.drop() // Drop rows with NULL values in ANY of these columns
    filteredCrimesTable.write.option("header",true).csv("hdfs://dicvmd2.ie.cuhk.edu.hk:8020/user/s1155160788/output.csv")
  }
}