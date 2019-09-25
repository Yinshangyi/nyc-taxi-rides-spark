import utils.SparkSessionBase
import dataProcessing._
import distance._
import getData._
import org.apache.spark.sql.functions.broadcast


object mainService extends SparkSessionBase {

  def main(args: Array[String]): Unit = {

    val trainDataLink = "data/train_data"
    val holidayDataLink = "data/usholidays.csv"
    val trainData = getTrainData(sparkSession, trainDataLink)
    val trainDataTimeFeatures = processTime(trainData)

    val holidayData = getHolidayData(sparkSession, holidayDataLink)

    var trainDataWithHoliday = trainDataTimeFeatures.join(broadcast(holidayData), Seq("Date"), "left_outer")
    trainDataWithHoliday = makeHolidays(trainDataWithHoliday)

    trainDataWithHoliday = makeHaversineDistance(trainDataWithHoliday)

    trainDataWithHoliday.show()

  }

}
