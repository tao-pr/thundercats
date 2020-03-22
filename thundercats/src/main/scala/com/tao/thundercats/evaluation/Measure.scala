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

trait Measure {
  def % (df: DataFrame): MayFail[Double]
}

trait RegressionMeasure extends Measure

/**
 * Calculate fitting error between real label and predicted output
 */
case class RMSE(override val df: DataFrame) extends RegressionMeasure {
  override def % (df: DataFrame): MayFail[Double] = MayFail {
    // TAOTODO Assert type of input
    val agg = new DoubleRDDFunctions(df
      .withColumn("sqe", pow(col(outputCol) - col(labelCol), 2.0))
      .rdd.map(_.getAs[Double]("sqe")))

    agg.mean.sqrt
  }
}

/**
 * Calculate fitting error between real label and predicted output
 */
case class MAE(override val df: DataFrame) extends RegressionMeasure {
  override def % (df: DataFrame): MayFail[Double] = MayFail {
    ???
  }
}

/**
 * Calculate correlation between input and real label
 */
case class PearsonCorr(
  override val df: DataFrame, 
  inputCol: String,
  labelCol: String) extends RegressionMeasure {
  override def % (df: DataFrame): MayFail[Double] = MayFail {
    // TAOTODO Assert type of input
    val rddX = df.rdd.map(_.getAs[Double](inputCol))
    val rddY = df.rdd.map(_.getAs[Double](labelCol))
    ExposedPearsonCorrelation.computeCorrelation(rddX, rddY)
  }
}

