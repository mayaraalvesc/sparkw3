package Dataset
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
//
// Create Datasets of primitive type and tuple type ands show simple operations.
//
object Basic {
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    val spark =
      SparkSession
        .builder()
        .master("local[2]")
        .appName("Dataset-Basic")
        .getOrCreate()
    import spark.implicits._
    // Create a tiny Dataset of integers
    val s = Seq(10, 11, 12, 13, 14, 15)
    val ds = s.toDS()
    ds.collect().foreach(e=>println(e))
    ds.show()
    println("*** columns and schema")
    ds.printSchema()
    println("*** column types")
    //ds.dtypes...
    ds.dtypes.foreach(println(_))
    println("*** schema as if it was a DataFrame")
    println("*** values > 12")
    ds.where(($"value" >12)).foreach(println(_))
    // This seems to be the best way to get a range that's actually a Seq and
    // thus easy to convert to a Dataset, rather than a Range, which isn't.
    val s2 = Seq.range(1, 100)
    println("*** size of the range = "+ s2.size)
    val tuples = Seq((1, "one", "un"), (2, "two", "deux"), (3, "three", "trois"))
    val tupleDS = tuples.toDS()
    tupleDS.show()
    println("*** Tuple Dataset types:")
    tupleDS.dtypes.foreach(println(_))
    // the tuple columns have unfriendly names, but you can use them to query
    println("*** filter by one column and fetch another")
    tupleDS.filter(x => x._1 == 1).show()
    tupleDS.filter(x => x._1 == 2).show()
    tupleDS.filter(x => x._1 == 3).show()


  }
}