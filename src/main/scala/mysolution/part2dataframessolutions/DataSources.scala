package mysolution.part2dataframessolutions

import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}

object DataSources extends App {

  val spark = SparkSession.builder()
    .appName("Data Sources and Formats")
    .config("spark.master", "local")
    .getOrCreate()

  /**
    * Exercise: read the movies DF, then write it as
    * - tab-separated values file
    * - snappy Parquet
    * - table "public.movies" in the Postgres DB
    */

  val moviesDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/movies.json")

  moviesDF.write
    .format("csv")
    .option("header", "true")
    .option("sep", "\t")
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/movies.csv")

  // snappy is the default compression
  moviesDF.write
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/movies.parquet")

  // Remote Postgres DB
  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"

  moviesDF.write
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", "public.movies")
    .mode(SaveMode.Overwrite)
    .save()


}
