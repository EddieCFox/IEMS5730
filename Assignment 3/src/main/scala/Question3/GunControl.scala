package Question3
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._ // import built in dataframe API functions

object GunControl {
  def main (args: Array[String]): Unit = {
    val spark = SparkSession // Create SparkSession
      .builder
      .appName("GunControl")
      .getOrCreate()
    val filePath = args(0).toString // Takes first argument and stores it as file path.

    val crimesTable = spark.read.format("csv") // read CSV formatted file
      .option("sep", ",") // comma separated
      .option("inferSchema", "true") // infer schema
      .option("header", "true") // csv file has header
      .load(filePath + "Crime_Incidents_in_*.csv") // load file corresponding to the first argument

    // Select only REPORT_DAT and records where method of crime is gun
    val focusedCrimesTable = crimesTable.select("REPORT_DAT", "METHOD")
                            .where("METHOD == 'GUN'")
    val filteredCrimesTable = focusedCrimesTable.na.drop() // Drop rows with NULL values in ANY of these columns

    // Extract year from the date in each column, make it its own column.
    val yearsTable = filteredCrimesTable.withColumn("Year", year(to_date(col("REPORT_DAT"), "yyyy/MM/dd")))
                  .groupBy("Year") // group by Year
                  .agg(count("METHOD").alias("Count")) // Sum up number of gun crimes per year
                  .withColumn("Percentage", col("Count").multiply(100) / sum("Count").over())
    // Output final results.
    val yearsTableOutput = yearsTable.select("Year", "Percentage")
                          .orderBy(asc("Year"))
    yearsTableOutput.show(false)
  }
}