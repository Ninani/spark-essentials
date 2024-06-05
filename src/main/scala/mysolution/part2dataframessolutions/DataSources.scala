package mysolution.part2dataframessolutions

import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}

object DataSources extends App {

  val spark = SparkSession.builder()
    .appName("Data Sources and Formats")
    .config("spark.master", "local")
    .getOrCreate()

//  val carsSchema = StructType(Array(
//    StructField("Name", StringType),
//    StructField("Miles_per_Gallon", DoubleType),
//    StructField("Cylinders", LongType),
//    StructField("Displacement", DoubleType),
//    StructField("Horsepower", LongType),
//    StructField("Weight_in_lbs", LongType),
//    StructField("Acceleration", DoubleType),
//    StructField("Year", DateType),
//    StructField("Origin", StringType)
//  ))
//
//  /*
//    Reading a DF:
//    - format
//    - schema or inferSchema = true
//    - path
//    - zero or more options
//   */
//  val carsDF = spark.read
//    .format("json")
//    .schema(carsSchema) // enforce a schema
//    .option("mode", "failFast") // dropMalformed, permissive (default)
//    .option("path", "src/main/resources/data/cars.json")
//    .load()
//
//  // alternative reading with options map
//  val carsDFWithOptionMap = spark.read
//    .format("json")
//    .options(Map(
//      "mode" -> "failFast",
//      "path" -> "src/main/resources/data/cars.json",
//      "inferSchema" -> "true"
//    ))
//    .load()
//
//  /*
//   Writing DFs
//   - format
//   - save mode = overwrite, append, ignore, errorIfExists
//   - path
//   - zero or more options
//  */
//  carsDF.write
//    .format("json")
//    .mode(SaveMode.Overwrite)
//    .save("src/main/resources/data/cars_dupe.json")
//
//  // JSON flags
//  spark.read
//    .schema(carsSchema)
//    .option("dateFormat", "yyyy-MM-dd") // couple with schema; if Spark fails parsing, it will put null
//    .option("allowSingleQuotes", "true")
//    .option("compression", "uncompressed") // bzip2, gzip, lz4, snappy, deflate
//    .json("src/main/resources/data/cars.json")
//
//  // CSV flags
//  val stocksSchema = StructType(Array(
//    StructField("symbol", StringType),
//    StructField("date", DateType),
//    StructField("price", DoubleType)
//  ))
//
//  spark.read
//    .schema(stocksSchema)
//    .option("dateFormat", "MMM d yyyy")
//    .option("header", "true")
//    .option("sep", ",")
//    .option("nullValue", "")
//    .csv("src/main/resources/data/stocks.csv")
//
//  // Parquet
//  carsDF.write
//    .mode(SaveMode.Overwrite)
//    .save("src/main/resources/data/cars.parquet")
//
//  // Text files
//  spark.read.text("src/main/resources/data/sampleTextFile.txt").show()

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
