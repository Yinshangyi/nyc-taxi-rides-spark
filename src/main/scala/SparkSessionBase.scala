package utils

trait SparkSessionBase {

  val sparkSession = org.apache.spark.sql.SparkSession.builder
    .master("local[*]")
    .appName("ScalaToolsLibrary")
    .getOrCreate()

  sparkSession.sparkContext.setLogLevel("ERROR")

}