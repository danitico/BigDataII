package metrics

import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql.DataFrame

class Metric {
  def getTPRTNR(labelAndPrediction: DataFrame): Double = {
    val evaluator = new MulticlassClassificationEvaluator()
    evaluator.setMetricName("truePositiveRateByLabel")

    val tnr = evaluator.evaluate(labelAndPrediction)
    val tpr = evaluator.setMetricLabel(1).evaluate(labelAndPrediction)

    tpr * tnr
  }
}
