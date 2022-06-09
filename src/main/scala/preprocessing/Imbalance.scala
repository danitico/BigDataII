package preprocessing

import org.apache.spark.sql.DataFrame

class Imbalance extends Serializable {
  private def getPositiveAndNegativeInstances(dataframe: DataFrame): (DataFrame, Double, DataFrame, Double) = {
    val positive = dataframe.filter(_.getInt(0) == 1)
    val negative = dataframe.filter(_.getInt(0) == 0)
    val num_pos = positive.count().toDouble
    val num_neg = negative.count().toDouble

    (positive, num_pos, negative, num_neg)
  }

  def ros(data: DataFrame, overRate: Double): DataFrame = {
    val (positive, num_pos, negative, num_neg) = this.getPositiveAndNegativeInstances(data)

    if (num_pos > num_neg) {
      val fraction = (num_pos * overRate) / num_neg
      positive.union(negative.sample(withReplacement = true, fraction))
    } else {
      val fraction = (num_neg * overRate) / num_pos
      negative.union(positive.sample(withReplacement = true, fraction))
    }
  }

  def rus(data: DataFrame): DataFrame = {
    val (positive, num_pos, negative, num_neg) = this.getPositiveAndNegativeInstances(data)

    if (num_pos > num_neg) {
      val fraction = num_neg / num_pos
      negative.union(positive.sample(withReplacement = false, fraction))
    } else {
      val fraction = num_pos / num_neg
      positive.union(negative.sample(withReplacement = false, fraction))
    }
  }
}
