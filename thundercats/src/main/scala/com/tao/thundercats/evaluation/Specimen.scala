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
 * Trained Model for evaluation
 */
trait Specimen {
  val model: PipelineModel
  val outputCol: String
  val labelCol: String

  /**
   * Ensure the dataframe is transformed before use
   */
  protected def ensure(df: DataFrame): DataFrame = {
    if (df.columns contains outputCol)
      df
    else
      model.transform(df)
  }

  /**
   * Score the specimen with the given data
   */
  def score(df: DataFrame, measure: Measure): MayFail[Double] = 
    measure % (ensure(df), this)
}

/**
 * Linear regression
 */
case class RegressionSpecimen(
  override val model: PipelineModel,
  featureCol: FeatureColumn,
  override val outputCol: String,
  override val labelCol: String
) extends Specimen {
  override def score(df: DataFrame, measure: Measure) = 
    measure match {
      case RMSE               => super.score(df, measure)
      case MAE                => super.score(df, measure)
      case PearsonCorr(input) => super.score(df, measure)
      case _                  => Fail(
        s"Unsupported measure type : ${measure.getClass.getName}")
    }
}
