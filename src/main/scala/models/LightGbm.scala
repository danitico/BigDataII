package models

import com.microsoft.azure.synapse.ml.lightgbm.{LightGBMClassifier, LightGBMClassificationModel}
import org.apache.spark.sql.DataFrame

class LightGbm {
  var _lightgbmModel: LightGBMClassificationModel = null

  def fit(train: DataFrame): Unit = {
    this._lightgbmModel = new LightGBMClassifier().setLabelCol(
      "label"
    ).setFeaturesCol(
      "features"
    ).setPredictionCol(
      "prediction"
    ).fit(train)
  }

  def transform(test: DataFrame): DataFrame = {
    this._lightgbmModel.transform(test)
  }
}
