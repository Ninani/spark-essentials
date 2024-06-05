package mysolution.part2dataframessolutions

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, max}
import part2dataframes.Joins.{employeesSalariesDF, titlesDF}

object Joins extends App {

  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()


  /**
    * Exercises
    *
    * 1. show all employees and their max salary
    * 2. show all employees who were never managers
    * 3. find the job titles of the best paid 10 employees in the company
    */

  // Remote Postgres DB
  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"

  def readTable(tableName: String) = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", s"public.$tableName")
    .load()

  // 1

  val employeesDF = readTable("employees")
  val salariesDF = readTable("salaries")

  val maxSalariesDF = salariesDF.groupBy("emp_no").agg(max("salary").as("max_salary"))
  val employeeSalaryDF = employeesDF.join(maxSalariesDF, "emp_no")
  employeeSalaryDF.show()

  // 2

  val titlesDF = readTable("titles")
  val deptManagersDF = readTable("dept_manager")

    val empNeverManagersDF = employeesDF.join(
      deptManagersDF,
      employeesDF.col("emp_no") === deptManagersDF.col("emp_no"),
      "left_anti"
    )
    empNeverManagersDF.show()

    // 3

  val mostRecentJobTitlesDF = titlesDF.groupBy("emp_no", "title").agg(max("to_date"))
  val bestPaidEmployeeDF = employeeSalaryDF.orderBy(col("maxSalary").desc).limit(10)
  val bestPaidJobsDF = bestPaidEmployeeDF.join(mostRecentJobTitlesDF, "emp_no")

  bestPaidJobsDF.show()



}

