package mysolution.part3typesdatasetssolutions

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object ComplexTypes extends App {

  val spark = SparkSession.builder()
    .appName("Complex Data Types")
    .config("spark.master", "local")
    .getOrCreate()

  /**
    * Exercise
    * 1. How do we deal with multiple date formats?
    * 2. Read the stocks DF and parse the dates
    */

  // 2
  val stocksDF = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/main/resources/data/stocks.csv")

  stocksDF
    .withColumn("actual_date", to_date(col("date"), "MMM d yyyy")).show



}
