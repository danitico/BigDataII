import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.feature.VectorAssembler
import preprocessing.{Imbalance, Scaler}
import metrics.Metric
import models.{DecisionTree, RandomForest, SVM, KNN}


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
    val transformed = assembler.transform(
      data
    ).drop(columns:_*).withColumnRenamed("_c18", "label")

    transformed.withColumn("label", transformed("label").cast("Integer"))
  }

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("Ranchal").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    val trainDatasetPath = "hdfs:/user/datasets/master/susy/susyMaster-Train.data"
    val testDatasetPath = "hdfs:/user/datasets/master/susy/susyMaster-Test.data"

    val train = readDataset(spark, trainDatasetPath)
    val test = readDataset(spark, testDatasetPath)

    val scaler = new Scaler("features")
    scaler.fit(train)
    val trainScaled = scaler.transform(train)
    val testScaled = scaler.transform(test)

    val evaluator = new Metric()
    val imbalance = new Imbalance()
    val trainRos = imbalance.ros(train, 1.0).cache()
    val trainRus = imbalance.rus(train).cache()
    val trainScaledRos = imbalance.ros(trainScaled, 1.0).cache()
    val trainScaledRus = imbalance.rus(trainScaled).cache()

    val classifier = new KNN()
    // metric Decision Tree -> 0.0
    // metric Random Forest -> 0.0
    // metric SVM -> 0.0
    // metric KNN -> 0.2105392879111111
    classifier.fit(train)
    println(evaluator.getTPRTNR(classifier.transform(test)))

    // metric Decision Tree -> 0.0
    // metric Random Forest -> 0.0
    // metric SVM -> 0.0
    // metric KNN -> 0.1410931137
    classifier.fit(trainScaled)
    println(evaluator.getTPRTNR(classifier.transform(testScaled)))

    // metric Decision Tree -> 0.5883544325555555
    // metric Random Forest -> 0.5901122982333333
    // metric SVM -> 0.5909708840222222
    // metric KNN -> 0.4708454414555555
    classifier.fit(trainRos)
    println(evaluator.getTPRTNR(classifier.transform(test)))

    // metric Decision Tree -> 0.5899042608666667
    // metric Random Forest -> 0.5899452314
    // metric SVM -> 0.5906741654
    // metric KNN -> 0.41811228600000006
    classifier.fit(trainScaledRos)
    println(evaluator.getTPRTNR(classifier.transform(testScaled)))

    // metric Decision Tree -> 0.5869293859888889
    // metric Random Forest -> 0.5879840650333333
    // metric SVM -> 0.5895903116
    // metric KNN -> 0.5629531208888889
    classifier.fit(trainRus)
    println(evaluator.getTPRTNR(classifier.transform(test)))

    // metric Decision Tree -> 0.5892960917333333
    // metric Random Forest -> 0.5906987809222223
    // metric SVM -> 0.5906299287
    // metric KNN -> 0.5013312101999999
    classifier.fit(trainScaledRus)
    println(evaluator.getTPRTNR(classifier.transform(testScaled)))

    spark.stop()
  }
}
