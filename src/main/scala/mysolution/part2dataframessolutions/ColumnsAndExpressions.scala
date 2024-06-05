package mysolution.part2dataframessolutions

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, expr}

object ColumnsAndExpressions extends App {

  val spark = SparkSession.builder()
    .appName("DF Columns and Expressions")
    .config("spark.master", "local")
    .getOrCreate()

  /**
    * Exercises
    *
    * 1. Read the movies DF and select 2 columns of your choice
    * 2. Create another column summing up the total profit of the movies = US_Gross + Worldwide_Gross + DVD sales
    * 3. Select all COMEDY movies with IMDB rating above 6
    *
    * Use as many versions as possible
    */

    // 1
  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  val selectedMoviesDF = moviesDF.select("Title", "IMDB_Rating")

  val selectedMoviesDF2 = moviesDF.select(
    col("Title"),
    col("IMDB_Rating")
  )

  val titleCol = moviesDF.col("Title")
  val ratingCol = moviesDF.col("IMDB_Rating")

  val selectedMoviesDF3 = moviesDF.select(titleCol, ratingCol)
  selectedMoviesDF3.show()

  // 2
  // "Total Profit = US_Gross + Worldwide_Gross " - DVD Sales omitted due to null values
  val moviesWithTotalProfitDF = moviesDF.withColumn("Total_Profit", col("US_Gross") + col("Worldwide_Gross"))
  moviesWithTotalProfitDF.show()

  // 3
  val comediesWithHighRatingDF = moviesDF.filter(col("Major_Genre") === "Comedy" and col("IMDB_Rating") > 6)
  val comediesWithHighRatingDF2 = moviesDF.filter("Major_Genre = 'Comedy' and IMDB_Rating > 6")
  comediesWithHighRatingDF2.show()

}
