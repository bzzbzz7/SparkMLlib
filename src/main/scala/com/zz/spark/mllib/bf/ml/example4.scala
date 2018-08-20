package com.zz.spark.mllib.bf

import com.zz.util.PathUtil
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.sql.SparkSession

//ParamGridBuilder TrainValidationSplit
object example4 {
  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder.
      master("local[2]")
      .appName("example4")
      .getOrCreate()
    val sqlContext = sparkSession

    val data_path = PathUtil.root + "/user/spark/mllib/bf/sample_linear_regression_data.txt"
    val data = sqlContext.read.format("libsvm").load(data_path)
    val Array(training, test) = data.randomSplit(Array(0.9, 0.1), seed = 12345)

    val lr = new LinearRegression()

    val paramGrid = new ParamGridBuilder().
      addGrid(lr.elasticNetParam, Array(0.0, 0.5, 1.0)).
      addGrid(lr.fitIntercept).
      addGrid(lr.regParam, Array(0.1, 0.01)).
      build()

    val trainValidationSplit = new TrainValidationSplit().
      setEstimator(lr).
      setEstimatorParamMaps(paramGrid).
      setEvaluator(new RegressionEvaluator).
      setTrainRatio(0.8)

    val model = trainValidationSplit.fit(training)

    model.transform(test).select("features", "label", "prediction").show()

  }
}
