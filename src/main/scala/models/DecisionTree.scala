package models

import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.sql.DataFrame

class DecisionTree {
  var _decisionTreeModel: DecisionTreeClassificationModel = null

  def fit(train: DataFrame): Unit = {
    this._decisionTreeModel = new DecisionTreeClassifier().setLabelCol(
      "label"
    ).setFeaturesCol(
      "features"
    ).setPredictionCol(
      "prediction"
    ).fit(train)
  }

  def transform(test: DataFrame): DataFrame = {
    this._decisionTreeModel.transform(test)
  }
}
