package preprocessing

import org.apache.spark.ml.feature.{MinMaxScaler, MinMaxScalerModel}
import org.apache.spark.sql.DataFrame

class Scaler extends Serializable {
  var _inputColumn: String = ""
  var _minMax: MinMaxScaler = new MinMaxScaler()
  var _minMaxModel: MinMaxScalerModel = null

  def this(inputColumn: String) = {
    this()
    this._inputColumn = inputColumn
    this._minMax.setInputCol(inputColumn).setOutputCol("scaled")
  }

  def fit(data: DataFrame): Unit = {
    this._minMaxModel = this._minMax.fit(data)
  }

  def transform(data: DataFrame): DataFrame = {
    this._minMaxModel.transform(
      data
    ).drop(
      this._inputColumn
    ).withColumnRenamed("scaled", this._inputColumn)
  }
}
