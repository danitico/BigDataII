package models

import org.apache.spark.ml.classification.{LinearSVCModel, LinearSVC}
import org.apache.spark.sql.DataFrame

class SVM {
  var _SVMModel: LinearSVCModel = null

  def fit(train: DataFrame): Unit = {
    this._SVMModel = new LinearSVC().setLabelCol(
      "label"
    ).setFeaturesCol(
      "features"
    ).setPredictionCol(
      "prediction"
    ).setMaxIter(10).setRegParam(0.1).fit(train)
  }

  def transform(test: DataFrame): DataFrame = {
    this._SVMModel.transform(test)
  }
}
