import utils.SparkSessionBase
import dataProcessing._
import distance._
import getData._
import clustering._
import farePrediction._
import org.apache.spark.sql.functions.broadcast


object mainService extends SparkSessionBase {

  def main(args: Array[String]): Unit = {

    val trainDataLink = "data/train.csv"
    val holidayDataLink = "data/usholidays.csv"
    val kmeansModelLink = "generated/kmeans.model"
    val trainKMeansBool = false

    /* Get training data */
    var trainData = getTrainData(sparkSession, trainDataLink)

    /* Make time related features */
    trainData = processTime(trainData)
    val holidayData = getHolidayData(sparkSession, holidayDataLink)
    trainData = trainData.join(broadcast(holidayData), Seq("Date"), "left_outer")
    trainData = makeHolidays(trainData)

    /* Make space related features */
    trainData = makeHaversineDistance(trainData)
    if(trainKMeansBool)
      trainKMeans(trainData, kmeansModelLink)

    trainData = predictClusters(trainData, kmeansModelLink)

    /* Filter outliers */
    trainData = filterOutliers(trainData)

    /* Train the Taxi fare prediction algorithm */
    trainFarePredictionModel(trainData)
  }


}
