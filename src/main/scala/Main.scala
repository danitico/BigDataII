import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.ml.feature.VectorAssembler

import preprocessing.{Imbalance, Scaler}

object Main {
  def readDataset(session: SparkSession, path: String): DataFrame = {
    val columns = Array(
      "_c0", "_c1", "_c2", "_c3", "_c4", "_c5",
      "_c6", "_c7", "_c8", "_c9", "_c10", "_c11",
      "_c12", "_c13", "_c14", "_c15", "_c16", "_c17"
    )

    val data = session.read.options(
      Map("header" -> "false", "inferSchema" -> "true")
    ).csv(path)

    val assembler = new VectorAssembler()
    assembler.setInputCols(columns).setOutputCol("features")

    /* Unpack columns. In python we use ** to unpack things */
    assembler.transform(data).drop(columns:_*).withColumnRenamed("_c18", "label")
  }

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("Ranchal").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    val trainDatasetPath = "hdfs:/user/datasets/master/susy/susyMaster-Train.data"
    val testDatasetPath = "hdfs:/user/datasets/master/susy/susyMaster-Test.data"

    try {
      val train = readDataset(spark, trainDatasetPath)
      val test = readDataset(spark, testDatasetPath)

      val scaler = new Scaler("features")
      scaler.fit(train)
      val trainScaled = scaler.transform(train)
      val testScaled = scaler.transform(test)

      trainScaled.show()
      testScaled.show()

      trainScaled.groupBy("label").count().show()

      val imbalance = new Imbalance()
      imbalance.ros(trainScaled, 1.0).groupBy("label").count().show()
      imbalance.rus(trainScaled).groupBy("label").count().show()
    } catch {
      case _: Throwable => spark.stop()
    }

    spark.stop()
  }
}
