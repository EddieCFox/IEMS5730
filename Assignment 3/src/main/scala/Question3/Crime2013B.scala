package Question3
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._ // import built in dataframe API functions

object Crime2013B {
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

    // Select only these 7 columns
    val focusedCrimesTable = crimesTable.select("CCN","REPORT_DAT", "OFFENSE", "METHOD", "END_DATE", "DISTRICT", "SHIFT")
    val filteredCrimesTable = focusedCrimesTable.na.drop() // Drop rows with NULL values in ANY of these columns

    val offenses = filteredCrimesTable.groupBy("OFFENSE") // group by offenses
                  .count() // count each type of offenses
                  .orderBy(desc("count")) // order by count in descending order
    // Removed show command for offenses because I am now showing time shift only.

    val timeShift = filteredCrimesTable.groupBy("SHIFT") // group by time shift crime occurred on
                  .count() // count each time shift
                  .orderBy(desc("count")) // order by count in descending order
    timeShift.show() // Show the most frequently occurring time shift and the count.
  }
}