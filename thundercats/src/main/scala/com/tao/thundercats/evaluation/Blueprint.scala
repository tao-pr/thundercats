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
 * Draft model
 * can be trained multiple times with different feature columns
 */
trait ModelDesign {
  val outputCol: String
  val labelCol: String
  def toSpecimen(feature: FeatureColumn, df: DataFrame): Specimen
}

case class DummyModelDesign(override val outputCol: String, override val labelCol: String) 
extends ModelDesign {
  override def toSpecimen(feature: FeatureColumn, df: DataFrame) = 
    DummySpecimen(feature, labelCol, outputCol) 
}

case class FeatureModelDesign(
  override val outputCol: String, 
  override val labelCol: String,
  estimator: Pipeline)
extends ModelDesign {
  override def toSpecimen(feature: FeatureColumn, df: DataFrame) = ??? // TAOTODO Train model
}
