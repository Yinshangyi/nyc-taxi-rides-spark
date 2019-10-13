import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

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

  def isHoliday(holiday: String): Boolean = {
    if (holiday != null)
      return true
    else
      return false
  }

  def isWeekend(dayOfTheWeek: Integer): Boolean = {
    if (dayOfTheWeek == 1 || dayOfTheWeek == 7)
      return true
    else
      return false
  }

  def makeHolidays(df: DataFrame): DataFrame = {
    val df_holidays = df.withColumn("isHoliday", isHolidayUDF(col("Holiday")))
      .withColumn("dayOfTheWeek", dayofweek(col("Date")))
      .withColumn("isWeekend", isWeekendUDF(col("dayOfTheWeek")))
    df_holidays
  }

  def filterOutliers(df: DataFrame): DataFrame = {
    val dfWithoutOutliers = df
      /* Number of passenger */
      .filter(col("passenger_count") < 6)

      /* Taxi Fare */
      .filter(col("fare_amount") < 80)
      .filter(col("fare_amount") > 0)

      /* Taxi locations for pickup */
      .filter(col("pickup_longitude") < -73)
      .filter(col("pickup_longitude") > -74.5)
      .filter(col("pickup_latitude") < 41.8)
      .filter(col("pickup_latitude") > 40.5)

      /* Taxi locations for dropoff */
      .filter(col("dropoff_longitude") < -73)
      .filter(col("dropoff_longitude") > -74.5)
      .filter(col("dropoff_latitude") < 41.8)
      .filter(col("dropoff_latitude") > 40.5)

    dfWithoutOutliers
  }

  val isHolidayUDF = udf(isHoliday _)
  val isWeekendUDF = udf(isWeekend _)

}


