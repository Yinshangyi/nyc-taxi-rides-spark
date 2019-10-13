import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.sql.functions.rand
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, StringIndexerModel, VectorAssembler}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object clustering {

  def makeClusteringDf(df: DataFrame): DataFrame ={
    val colsToIndex = Array("pickup_latitude", "pickup_longitude", "dropoff_latitude", "dropoff_longitude")

    val vectorizedDf = new VectorAssembler()
      .setInputCols(colsToIndex)
      .setOutputCol("features")
      .setHandleInvalid("skip")
      .transform(df)

    vectorizedDf
  }

  def trainKMeans(df: DataFrame, saveModelPath: String): Unit = {
    val vectorizedDf = makeClusteringDf(df)

    val kmeans = new KMeans()
      .setK(10)
      .setSeed(1L)
      .setFeaturesCol("features")
      .fit(vectorizedDf.sample(true, 0.0005))

    kmeans.save(saveModelPath)
  }

  def predictClusters(df: DataFrame, saveModelPath: String): DataFrame = {
    val vectorizedDf = makeClusteringDf(df)
    val model = KMeansModel.load(saveModelPath)
    val dfWithClusters = model.transform(vectorizedDf)
        .withColumnRenamed("prediction", "clusters")
    dfWithClusters
  }


}
