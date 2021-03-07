package org.apache.spark.examples.sql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object SparkSQLExample {
  Logger.getLogger("org.apache.spark").setLevel(Level.OFF)

  case class Person(name: String, age: Long)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate()

    runBasicDataFrameExample(spark)
    runDatasetCreationExample(spark)
    runInferSchemaExample(spark)
    runProgrammaticSchemaExample(spark)

    spark.stop()
  }

  private def runBasicDataFrameExample(spark: SparkSession): Unit = {
    val df = spark.read.json("people.json")
    df.show() // Displays the content of the DataFrame to stdout
    df.printSchema() // Print the schema in a tree format
    import spark.implicits._
    df.select("name").show() // Select only the "name" column
    df.select($"name", $"age" + 1).show // Select everybody, but increment the age by 1
    df.filter($"age" > 21).show() // Select people older than 21
    df.groupBy("age").count().show() // Count people by age
    df.createOrReplaceTempView("people") // Register the DataFrame as a SQL temporary view
    val sqlDF = spark.sql("SELECT * FROM people")
    sqlDF.show()
    df.createGlobalTempView("people") // Register the DataFrame as a global temporary view
    spark.sql("SELECT * FROM global_temp.people").show()
  }
  private def runDatasetCreationExample(spark: SparkSession): Unit = {
    import spark.implicits._
    val caseClassDS = Seq(Person("Andy", 32)).toDS // Encoders are created for case classes
    caseClassDS.show()
    val primitiveDS = Seq(1, 2, 3).toDS() // Encoders for most common types are automatically provided by importing spark.implicits._
    primitiveDS.map(_+ 1).collect() //Returns: array(2, 3, 4)
    val path = "people.json" // DataFrames can be converted to a Dataset by providing a class. Mapping will be done by name
    val peopleDS = spark.read.json(path).as[Person]
    peopleDS.show()
  }
  private def runInferSchemaExample(spark: SparkSession): Unit = {
    import spark.implicits._
    val peopleDF = spark.sparkContext
      .textFile("people.txt")
      .map(_.split(","))
      .map(attributes => Person(attributes(0), attributes(1).trim.toInt))
      .toDF()

    peopleDF.createOrReplaceTempView("people")// Register the DataFrame as a temporary view
    val teenagersDF = spark.sql("SELECT name, age FROM people WHERE age BETWEEN 13 AND 19")// SQL statements can be run by using the sql methods provided by Spark
    teenagersDF.map(teenager => "Name: " + teenager(0)).show()// The columns of a row in the result can be accessed by field index
    teenagersDF.map(teenager => "Name: " + teenager.getAs[String]("name")).show()// or by field name
    implicit val mapEncoder =  org.apache.spark.sql.Encoders.kryo[Map[String, Any]]
    teenagersDF.map(teenager => teenager.getValuesMap[Any](List("name", "age"))).collect()
  }
  private def runProgrammaticSchemaExample(spark: SparkSession): Unit = {
    import spark.implicits._
    val peopleRDD = spark.sparkContext.textFile("people.txt")  // Create an RDD
    val schemaString = "name age"// The schema is encoded in a string
    val fields = schemaString.split(" ") // Generate the schema based on the string of schema
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)
    val rowRDD = peopleRDD// Convert records of the RDD (people) to Rows
      .map(_.split(","))
      .map(attributes => Row(attributes(0), attributes(1).trim))
    val peopleDF = spark.createDataFrame(rowRDD, schema)// Apply the schema to the RDD
    val results = spark.sql("SELECT name FROM people")// SQL can be run over a temporary view created using DataFrames
    peopleDF.createOrReplaceTempView("people")// Creates a temporary view using the DataFrame
    results.map(attributes => "Name: " + attributes(0)).show()
  }
}