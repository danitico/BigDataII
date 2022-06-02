package preprocessing

import org.apache.spark.sql.DataFrame

class Imbalance extends Serializable {
  def ros(data: DataFrame, overRate: Double): DataFrame = {
    val train_positive = data.filter(_.getInt(18) == 1)
    val train_negative = data.filter(_.getInt(18) == 0)

    val num_pos = train_positive.count().toDouble
    val num_neg = train_negative.count().toDouble

    if (num_pos > num_neg) {
      val fraction = (num_pos * overRate) / num_neg
      train_positive.union(train_negative.sample(withReplacement = true, fraction))
    } else {
      val fraction = (num_neg * overRate) / num_pos
      train_negative.union(train_positive.sample(withReplacement = true, fraction))
    }
  }

  def rus(data: DataFrame): DataFrame = {
    val train_positive = data.filter(_.getInt(18) == 1)
    val train_negative = data.filter(_.getInt(18) == 0)

    val num_pos = train_positive.count().toDouble
    val num_neg = train_negative.count().toDouble

    if (num_pos > num_neg) {
      val fraction = num_neg / num_pos
      train_negative.union(train_positive.sample(withReplacement = false, fraction))
    } else {
      val fraction = num_pos / num_neg
      train_positive.union(train_negative.sample(withReplacement = false, fraction))
    }
  }
}
