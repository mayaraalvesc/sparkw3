package org.apache.spark.examples.sql
import org.apache.log4j.{Level, Logger}
// $example on:init_session$
import org.apache.spark.sql.SparkSession
// $example off:init_session$
// One method for defining the schema of an RDD is to make a case class with the desired column
// names and types.
case class Record(key: Int, value: String)
object RDDRelation {
  def main(args: Array[String]): Unit = {
    // $example on:init_session$
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    val spark = SparkSession
      .builder
      .master("local[2]")
      .appName("Spark Examples")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
//     Importing the SparkSession gives access to all the SQL functions and implicit conversions.
     //$example off:init_session$
    val df = spark.createDataFrame((1 to 100).map(i => Record(i, s"val_$i")))
    // Any RDD containing case classes can be used to create a temporary view.  The schema of the
    // view is automatically inferred using scala reflection.
    df.createOrReplaceTempView("records")
       // Once tables have been registered, you can run SQL queries over them.
    println("Result of SELECT *:")
    spark.sql("SELECT * from records WHERE").collect().foreach(println)
    //Select records<35
        //spark.sql("SELECT * from records WHERE key < 35").collect().foreach(println)
    // Aggregation queries are also supported.
//  val count = spark.sql("SELECT count(*) as count from records").collect().head.getLong(0)
//    println(s"COUNT(*): $count")
    //Calculate average
    spark.sql("SELECT AVG(Key) FROM records").show()
    // select records with #1
    spark.sql("SELECT(*) FROM records WHERE key like '%1%'").show()
      // The results of SQL queries are themselves RDDs and support all normal RDD functions. The
    // items in the RDD are of type Row, which allows you to access each column by ordinal.
    val rddFromSql = spark.sql("SELECT key, value FROM records WHERE key < 10")
    println("Result of RDD.map:")
    rddFromSql.rdd.map(row => s"Key: ${row(0)}, Value: ${row(1)}").collect().foreach(println)
    // Queries can also be written using a LINQ-like Scala DSL.
    spark.stop()
  }
}