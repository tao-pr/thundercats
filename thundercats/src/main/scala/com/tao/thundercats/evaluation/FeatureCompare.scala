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
 * Representations of feature columns
 */
trait FeatureColumn {
  /**
   * Take the features out of the input dataframe
   */
  def %(df: DataFrame, cols: String*): DataFrame
}

case class Feature(c: String) extends FeatureColumn {
  override def %(df: DataFrame, cols: String*) = df.select(c, cols:_*)
}

case class AssemblyFeature(cs: Seq[String], outputCol: String="features") extends FeatureColumn {
  def assembly(df: DataFrame, cols: String*): DataFrame = {
    new VectorAssembler()
      .setInputCols(cs.toArray)
      .setOutputCol(outputCol)
      .transform(df)
      .select(outputCol, cols:_*)
  }
  override def %(df: DataFrame, cols: String*) = assembly(df, cols:_*)
}


/**
 * Feature comparison suite
 */
trait FeatureCompare[A <: Score] extends Score {
  val baseFeature: FeatureColumn
  val newFeature: FeatureColumn

  override val model: PipelineModel = ???
  override val outputCol: String = ???
  override val labelCol: String = ???
}