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

import java.io.File
import java.lang.IllegalArgumentException
import sys.process._
import scala.reflect.io.Directory
import scala.util.Try

import com.tao.thundercats.physical._
import com.tao.thundercats.functional._
import com.tao.thundercats.physical.Implicits._
import com.tao.thundercats.estimator._

/**
 * Base model score representation for evaluation
 */
trait Score {
  val model: PipelineModel
  val outputCol: String
  val labelCol: String
  def betterThan(baseModel: Score): Boolean
}

/**
 * Suitable for regression
 * Estimate expects the following:
 * - Double label
 * - Double output
 */
trait RegressionScore extends Score {

  /**
   * Root mean square error of the estimate
   * Smaller value : better model
   */
  def rmse(df: DataFrame): MayFail[Double] = MayFail {
    val agg = new DoubleRDDFunctions(df
      .withColumn("sqe", pow(col(outputCol) - col(labelCol), 2.0))
      .rdd.map(_.getAs[Double]("sqe")))

    agg.mean.sqrt
  }

  /**
   * Pearson correlation between input and labels
   * Bigger value : better model
   */
  def pearsonCorr(df: DataFrame, inputCol: String): MayFail[Double] = MayFail {
    val rddX = df.rdd.map(_.getAs[Double](inputCol))
    val rddY = df.rdd.map(_.getAs[Double](labelCol))
    ExposedPearsonCorrelation.computeCorrelation(rddX, rddY)
  }

  /**
   * Calculate Z-score of standard error of estimate
   * Bigger value : better model
   */
  def zscore(df: DataFrame, inputCol: String): MayFail[Double] = MayFail {
    ??? // z = coeff / sigma * sqrt(v)
  }
}
