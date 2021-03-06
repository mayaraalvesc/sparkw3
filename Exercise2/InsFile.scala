package Dataset
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object InsFile {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    val spark = SparkSession
      .builder
      .master("local[2]")
      .appName("read_csv")
      .getOrCreate()
    //define dataframe and read csv file with header
    val df = spark.read.option("header", true).csv("src/main/scala/insurance.csv")
    df.createOrReplaceTempView("insurance")
    //   Data size
    def RDDSize(rdd: RDD[String]): Long = {
      rdd.map(_.getBytes("UTF-8").length.toLong).reduce(_+_)
    }
    val rdd_df = df.rdd.map(_.toString())
    val size = RDDSize(rdd_df)
    println("Data size: " + size)
    //   Sex and count of sex
    spark.sql("SELECT sex, COUNT(*) as count FROM insurance GROUP BY sex").show()
    // Count of smokers by sex
    spark.sql("SELECT sex, COUNT(*) as smokers FROM insurance WHERE smoker='yes' GROUP BY sex").show()
    //Charges by region
    spark.sql("SELECT region, sum(charges) as charges from insurance GROUP BY region ORDER BY charges desc").show()
  }
}