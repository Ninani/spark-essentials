package mysolution.part3typesdatasetssolutions

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}
import part3typesdatasets.Datasets.readDF


object Datasets extends App {

  val spark = SparkSession.builder()
    .appName("Datasets")
    .config("spark.master", "local")
    .getOrCreate()

  // dataset of a complex type
  // 1 - define your case class
  case class Car(
                Name: String,
                Miles_per_Gallon: Option[Double],
                Cylinders: Long,
                Displacement: Double,
                Horsepower: Option[Long],
                Weight_in_lbs: Long,
                Acceleration: Double,
                Year: String,
                Origin: String
                )

  // 2 - read the DF from the file
  def readDF(filename: String) = spark.read
    .option("inferSchema", "true")
    .json(s"src/main/resources/data/$filename")

  val carsDF = readDF("cars.json")

  // 3 - define an encoder (importing the implicits)
  import spark.implicits._
  // 4 - convert the DF to DS
  val carsDS = carsDF.as[Car]

  /**
    * Exercises
    *
    * 1. Count how many cars we have
    * 2. Count how many POWERFUL cars we have (HP > 140)
    * 3. Average HP for the entire dataset
    */

// 1
  val carCount = carsDS.count()
  println(carCount)

  // 2
  val powerfulCarsCount = carsDS.filter(_.Horsepower.getOrElse(0L) > 140).count()
  println(powerfulCarsCount)

  // 3
  val avgHP = carsDS.map(_.Horsepower.getOrElse(0L)).reduce(_ + _) / carCount
  println(avgHP)



  // Joins
  case class Guitar(id: Long, make: String, model: String, guitarType: String)
  case class GuitarPlayer(id: Long, name: String, guitars: Seq[Long], band: Long)
  case class Band(id: Long, name: String, hometown: String, year: Long)

  val guitarsDS = readDF("guitars.json").as[Guitar]
  val guitarPlayersDS = readDF("guitarPlayers.json").as[GuitarPlayer]
  val bandsDS = readDF("bands.json").as[Band]

  val guitarPlayerBandsDS: Dataset[(GuitarPlayer, Band)] = guitarPlayersDS.joinWith(bandsDS, guitarPlayersDS.col("band") === bandsDS.col("id"), "inner")
  /**
    * Exercise: join the guitarsDS and guitarPlayersDS, in an outer join
    * (hint: use array_contains)
    */

  val guitarGuitarPlayersDS = guitarsDS.joinWith(guitarPlayersDS, array_contains(guitarPlayersDS.col("guitars"), guitarsDS.col("id")), "outer")
  guitarGuitarPlayersDS.show()

}
