// Exercise 3
package Dataset
import org.apache.spark.sql.SparkSession
object CaseClass {

  case class Number(i: Int, english: String, french: String)

  def main(args: Array[String]) {
    val spark =
      SparkSession.builder()
        .appName("Dataset-CaseClass")
        .master("local[4]")
        .getOrCreate()
    import spark.implicits._
    val numbers = Seq(
      Number(1, "one", "un"),
      Number(2, "two", "deux"),
      Number(3, "three", "trois"))
    val numberDS = numbers.toDS()
    println("Dataset Types")
    numberDS.dtypes.foreach(println(_))
    println("filter dataset where i>1")
    numberDS.where($"i" > 1).foreach(println(_))
    println("select the number with English column and display")
    numberDS.select("i", "english").show()
    println("select the number with English column and filter for i>1")
    numberDS.select("i", "english" ).where($"i">1).show()
    println("sparkSession dataset")
    val anotherDS = spark.createDataset(numbers)
    anotherDS.show()
    println("Spark Dataset Types")
    anotherDS.dtypes.foreach(println(_))

  }
}