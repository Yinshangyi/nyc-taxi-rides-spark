import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import java.util.concurrent.TimeUnit

object dataProcessing {

  def processTime(df: DataFrame): DataFrame = {

    val df_time_ts = df.withColumn("pickup_datetime", unix_timestamp(col("pickup_datetime"),
      "yyyy-MM-dd HH:mm:ss").cast(TimestampType))
      .withColumn("year", year(col("pickup_datetime")))
      .withColumn("month", month(col("pickup_datetime")))
      .withColumn("day", dayofmonth(col("pickup_datetime")))
      .withColumn("hour", hour(col("pickup_datetime")))
      .withColumn("hour", hour(col("pickup_datetime")))
      .withColumn("minute", minute(col("pickup_datetime")))
      .withColumn("Date", to_date(col("pickup_datetime")))
      .withColumn("pickup_time", round(col("hour") + col("minute")/60, 2))
    df_time_ts
  }

  def isHoliday(holiday: String): String = {
    if (holiday != null)
      return "True"
    else
      return "False"
  }

  def isWeekend(dayOfTheWeek: String): String = {
    if (dayOfTheWeek == "1" || dayOfTheWeek == "7")
      return "True"
    else
      return "False"
  }


  def makeHolidays(df: DataFrame): DataFrame = {
    val df_holidays = df.withColumn("isHoliday", isHolidayUDF(col("Holiday")))
      .withColumn("dayOfTheWeek", dayofweek(col("Date")))
      .withColumn("isWeekend", isWeekendUDF(col("dayOfTheWeek")))
    df_holidays
  }

  val isHolidayUDF = udf(isHoliday _)
  val isWeekendUDF = udf(isWeekend _)

}


