package part2dataframes

import org.apache.spark.sql.SparkSession
import part2dataframes.DataFramesBasics.spark

object DataFramesBasicsExercise extends App {

  // creating a SparkSession
  val spark = SparkSession.builder()
    .appName("DataFrames Basics")
    .config("spark.master", "local")
    .getOrCreate()

  /**
    * Exercise:
    * 1) Create a manual DF describing smartphones
    *   - make
    *   - model
    *   - screen dimension
    *   - camera megapixels
    *
    * 2) Read another file from the data/ folder, e.g. movies.json
    *   - print its schema
    *   - count the number of rows, call count()
    */

  var phones = Seq(
    ("Samsung", "Galaxy S21", 6.2, 12),
    ("Apple", "iPhone 12", 6.1, 12),
    ("Google", "Pixel 5", 6.0, 12),
    ("OnePlus", "8T", 6.55, 48),
    ("Xiaomi", "Mi 11", 6.81, 108)
  )

  import spark.implicits._
  val manualPhonesDf = phones.toDF("Make", "Model", "ScreenDimension", "CameraMegapixels")
  manualPhonesDf.show()

  val moviesDf = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/movies.json")

  moviesDf.printSchema()

  println(s"Number of rows in moviesDf: ${moviesDf.count()}")




}
