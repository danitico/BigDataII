package models

import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.classification.kNN_IS.{kNN_ISClassifier, kNN_ISClassificationModel}

class KNN {
  var _knnClassifier: kNN_ISClassificationModel = null

  def fit(train: DataFrame): Unit = {
    val k = 5
    val distance = 2 //euclidean
    val numClass = 2
    val numFeatures = 18
    val numPartitionMap = 10
    val numReduces = 2
    val numIterations = 10
    val maxWeight = 5
    val outPathArray=Array(".")

    this._knnClassifier = new kNN_ISClassifier().setLabelCol("label")
      .setFeaturesCol("features")
      .setK(k)
      .setDistanceType(distance)
      .setNumClass(numClass)
      .setNumFeatures(numFeatures)
      .setNumPartitionMap(numPartitionMap)
      .setNumReduces(numReduces)
      .setNumIter(numIterations)
      .setMaxWeight(maxWeight)
      .setNumSamplesTest(1000000)
      .setOutPath(outPathArray).fit(train)
  }

  def transform(test: DataFrame): DataFrame = {
    this._knnClassifier.transform(test)
  }
}
