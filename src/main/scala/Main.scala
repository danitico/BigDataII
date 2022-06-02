import org.apache.spark.sql.{SparkSession, DataFrame}

object Main {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("Ranchal").getOrCreate()

    try {
      val train = spark.read.format(
        "csv"
      ).option(
        "inferSchema", "true"
      ).option(
        "header", "false"
      ).load(
        "hdfs:/user/datasets/master/susy/susyMaster-Train.data"
      )

      train.show()
    } catch {
      case _: Throwable => spark.stop()
    }

    spark.stop()
  }
}
