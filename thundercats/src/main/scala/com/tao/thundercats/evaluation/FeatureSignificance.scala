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
import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.ml.param._
import org.apache.spark.ml.regression.LinearRegression
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

sealed trait FeatureSignificance[T <: Metric] extends Significance[T] {
  val featureCol: String
  val outputCol: String
  val labelCol: String
}

case class LinearModelFeatureSig[T <: LinearMetric](
  override val featureCol: String,
  override val outputCol: String,
  override val labelCol: String
) extends FeatureSignificance[T] {

  /**
   * Calculate a root mean square error of the prediction.
   * PREREQUISITES: The model has to be [[LinearRegression]]
   */
  def rmse(df: DataFrame, model: PipelineModel): MayFail[Double] = MayFail {
    
    val agg = new DoubleRDDFunctions(df
      .withColumn("sqe", pow(col(outputCol) - col(labelCol), 2.0))
      .rdd.map(_.getAs[Double]("sqe")))

    agg.mean.sqrt
  }

  override def measure(df: DataFrame, model: PipelineModel): LinearMetric = ???
}

/**
 * Base metric of feature for linear model
 */
sealed trait LinearMetric extends Metric

case class StandardError(e: Double) extends LinearMetric {
  override def > (m: Metric): Boolean = m match {
    case StandardError(e_) => e < e_
    case _ => throw new IllegalArgumentException(s"Cannot compare ${this.getClass.getName} with ${m.getClass.getName}")
  }

  def toZScore: ZScore = ???
}
case class ZScore(e: Double) extends LinearMetric {
  override def > (m: Metric): Boolean = m match {
    case ZScore(e_) => e > e_
    case _ => throw new IllegalArgumentException(s"Cannot compare ${this.getClass.getName} with ${m.getClass.getName}")
  }
}

case class FeatureConfidenceInterval(a: Double, b: Double) extends LinearMetric {
  override def > (m: Metric): Boolean = m match {
    case FeatureConfidenceInterval(u,v) => a>u && b>v
    case _ => throw new IllegalArgumentException(s"Cannot compare ${this.getClass.getName} with ${m.getClass.getName}")
  }
}

