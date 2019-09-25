import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object distance {

  def makeHaversineDistance(df: DataFrame): DataFrame = {
    val AVG_EARTH_RADIUS = 6371
    val dfHaversine = df.withColumn("pickup_latitude_rad", radians(col("pickup_latitude")))
      /* Convert latitudes and longitudes columns in radians */
      .withColumn("pickup_longitude_rad", radians(col("pickup_longitude")))
      .withColumn("dropoff_latitude_rad", radians(col("dropoff_latitude")))
      .withColumn("dropoff_longitude_rad", radians(col("dropoff_longitude")))

      /* Calculate the latitude and the longitude difference between pickup and dropoff */
      .withColumn("lat_diff", col("dropoff_latitude_rad") - col("pickup_latitude_rad"))
      .withColumn("lng_diff", col("dropoff_longitude_rad") - col("pickup_longitude_rad"))

      /* Calculate the Haversine formula */
      .withColumn("haversine", computeHaversineUDF(col("lat_diff"),col("pickup_latitude_rad"),
        col("dropoff_latitude_rad"),col("lng_diff"),lit(AVG_EARTH_RADIUS)))
    dfHaversine

  }

  def computeHaversine(lat_diff : Double, pickup_latitude_rad : Double, dropoff_latitude_rad : Double, lng_diff : Double, AVG_EARTH_RADIUS : Int) : Double = {
    val temp = scala.math.pow(scala.math.sin(lat_diff)*0.5,2) +
      (scala.math.cos(pickup_latitude_rad) * scala.math.cos(dropoff_latitude_rad) * scala.math.pow(scala.math.sin(lng_diff) * 0.5, 2))
    val result = scala.math.asin(temp) * (2*AVG_EARTH_RADIUS)
    result
  }

  val computeHaversineUDF = udf(computeHaversine _)

}
