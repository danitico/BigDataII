package models

import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.sql.DataFrame

class RandomForest {
  var _randomForestModel: RandomForestClassificationModel = null

  def fit(train: DataFrame): Unit = {
    this._randomForestModel = new RandomForestClassifier().setLabelCol(
      "label"
    ).setFeaturesCol(
      "features"
    ).setPredictionCol(
      "prediction"
    ).setNumTrees(10).fit(train)
  }

  def transform(test: DataFrame): DataFrame = {
    this._randomForestModel.transform(test)
  }
}
