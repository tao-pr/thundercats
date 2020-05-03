package com.tao.thundercats.evaluation

import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Column,Row}
import org.apache.spark.sql.catalyst.encoders._
import org.apache.spark.sql.{Encoders, Encoder}
import org.apache.spark.sql.avro._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import org.apache.spark.ml.feature.{HashingTF, Tokenizer, VectorAssembler}
import org.apache.spark.ml.{Transformer, PipelineModel}
import org.apache.spark.ml.{Pipeline, Estimator, PipelineStage}
import org.apache.spark.ml.{Predictor}
import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.ml.param._
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.mllib.stat.correlation.ExposedPearsonCorrelation
import org.apache.spark.rdd.DoubleRDDFunctions
import org.apache.spark.ml.classification._

import breeze.linalg.DenseVector

import com.tao.thundercats.physical._
import com.tao.thundercats.functional._
import com.tao.thundercats.physical.Implicits._
import com.tao.thundercats.estimator._


sealed trait BaseMeasure[A] {
  def % (df: DataFrame, specimen: Specimen): MayFail[A]
}

/**
 * Overall measure of the whole specimen
 */
trait Measure extends BaseMeasure[Double] {
  override def % (df: DataFrame, specimen: Specimen): MayFail[Double]
  def isBetter(a: Double, b: Double) = a > b
}

trait RegressionMeasure extends Measure

/**
 * Calculate fitting error between real label and predicted output.
 * Metric: Root mean square error
 */
case object RMSE
extends RegressionMeasure {
  override def % (df: DataFrame, specimen: Specimen): MayFail[Double] = MayFail {
    import specimen._
    val agg = new DoubleRDDFunctions(df
      .withColumn("sqe", pow(col(outputCol) - col(labelCol), 2.0))
      .rdd.map(_.getAs[Double]("sqe")))

    agg.mean.sqrt
  }

  override def isBetter(a: Double, b: Double) = a < b
}

/**
 * Calculate fitting error between real label and predicted output.
 * Metric: Mean absolute error
 */
case object MAE 
extends RegressionMeasure {
  override def % (df: DataFrame, specimen: Specimen): MayFail[Double] = MayFail {
    import specimen._
    val agg = new DoubleRDDFunctions(df
      .withColumn("mae", abs(col(outputCol) - col(labelCol)))
      .rdd.map(_.getAs[Double]("mae")))
    agg.mean
  }

  override def isBetter(a: Double, b: Double) = a < b
}

/**
 * Calculate correlation between input and real label
 */
case object PearsonCorr extends RegressionMeasure {
  override def % (df: DataFrame, specimen: Specimen): MayFail[Double] = MayFail {
    import specimen._
    val rddX = df getDoubleRDD outputCol
    val rddY = df getDoubleRDD labelCol
    ExposedPearsonCorrelation.computeCorrelation(rddX, rddY)
  }
}


