package com.tao.thundercats.model

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

import java.io.File
import sys.process._
import scala.reflect.io.Directory
import scala.util.Try

import com.tao.thundercats.physical._
import com.tao.thundercats.functional._
import com.tao.thundercats.physical.Implicits._
import com.tao.thundercats.estimator._

sealed trait FeatureSignificance[T <: Metric] extends Significance[T] {
  val featureCol: String
}

case class LinearModelFeatureSig[T <: LinearMetric](
  override val featureCol: String
) extends FeatureSignificance[T] {

  override def measure(df: DataFrame, model: Pipeline): Metric = ???
}

sealed trait LinearMetric extends Metric
case class StandardError(e: Double) extends LinearMetric {
  override def > (b: Metric): Boolean = b match {
    case StandardError(e_) => e < e_
    case _ => ???
  }
}
case class ZScore(e: Double) extends LinearMetric {
  override def > (b: Metric): Boolean = b match {
    case ZScore(e_) => e > e_
    case _ => ???
  }
}
