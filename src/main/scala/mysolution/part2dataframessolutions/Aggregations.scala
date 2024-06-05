package mysolution.part2dataframessolutions

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Aggregations extends App {

  val spark = SparkSession.builder()
    .appName("Aggregations and Grouping")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")


  /**
    * Exercises
    *
    * 1. Sum up ALL the profits of ALL the movies in the DF
    * 2. Count how many distinct directors we have
    * 3. Show the mean and standard deviation of US gross revenue for the movies
    * 4. Compute the average IMDB rating and the average US gross revenue PER DIRECTOR
    */

  // 1
  moviesDF.selectExpr("sum(US_Gross + Worldwide_Gross + US_DVD_Sales) as Total_Profit").show()

  // 2
  moviesDF.select(countDistinct(col("Director"))).show()

  // 3
  moviesDF.select(
    mean(col("US_Gross")),
    stddev(col("US_Gross"))
  ).show()

  // 4
  moviesDF.groupBy(col("Director"))
    .agg(
      avg("IMDB_Rating").as("Avg_IMDB_Rating"),
      avg("US_Gross").as("Avg_US_Gross")
    )
    .orderBy(col("Avg_IMDB_Rating").desc_nulls_last)
    .show()




}
