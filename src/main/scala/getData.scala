import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.functions._

object getData {

  def getTrainData(sparkSession: SparkSession, link: String): DataFrame = {

    val customSchema = StructType(Array(
      StructField("key", StringType, true),
      StructField("fare_amount", DoubleType, true),
      StructField("pickup_datetime", StringType, true),
      StructField("pickup_longitude", DoubleType, true),
      StructField("pickup_latitude", DoubleType, true),
      StructField("dropoff_longitude", DoubleType, true),
      StructField("dropoff_latitude", DoubleType, true),
      StructField("passenger_count", IntegerType, true)))

    val df = sparkSession.read
      .format("csv")
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .schema(customSchema)
      .load(link)
      .withColumn("pickup_datetime", unix_timestamp(col("pickup_datetime"),
        "yyyy-MM-dd HH:mm:ss").cast(TimestampType))
    df
  }

  def getHolidayData(sparkSession: SparkSession, link: String): DataFrame = {
    val df = sparkSession.read.format("csv")
      .option("header", "true")
      .load(link)
      .withColumn("Date", to_date(col("Date")))
    df
  }

}
