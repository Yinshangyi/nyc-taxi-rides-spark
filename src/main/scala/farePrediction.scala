import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.GBTRegressor


object farePrediction {

  def trainFarePredictionModel(df: DataFrame): Unit = {

    val dfSample = df.sample(true, 0.0005)

    /* Split data into training and validation sets */
    val Array(trainingSet, validationSet) = dfSample.randomSplit(Array(0.8, 0.2))

    val keepFeatures = Array("passenger_count", "year", "month", "day", "hour", "minute", "isHoliday",
      "pickup_longitude", "pickup_latitude", "dropoff_longitude", "dropoff_latitude",
      "dayOfTheWeek", "isWeekend", "haversine", "clusters")

    val keepFeaturesCols = keepFeatures.map(colName => col(colName))

    val assembler = new VectorAssembler()
      .setInputCols(keepFeatures)
      .setOutputCol("features_regression")
      .setHandleInvalid("skip")

    val gbtClassifier = new GBTRegressor()
      .setLabelCol("fare_amount")
      .setFeaturesCol("features_regression")
      .setPredictionCol("predicted_fare")
      .setMaxBins(10)
      .setMaxIter(10)

    val stages = Array(
      assembler,
      gbtClassifier
    )

    val pipeline = new Pipeline().setStages(stages)

    val model = pipeline.fit(trainingSet)

    val predictions = model.transform(validationSet)

    val evaluator = new RegressionEvaluator()
      .setLabelCol("fare_amount")
      .setPredictionCol("predicted_fare")
      .setMetricName("rmse")

    val rmse = evaluator.evaluate(predictions)

    println(s"Root Mean Squared Error (RMSE) on test data = $rmse")

  }

}
