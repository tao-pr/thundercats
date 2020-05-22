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
  val featureCol: FeatureColumn

  /**
   * Ensure the dataframe is transformed before use
   */
  protected def ensure(df: DataFrame): DataFrame = {
    if (df.columns contains outputCol)
      df
    else{
      // REVIEW: Log that the transformation is triggered
      val trans = model.transform(df)
      trans.show(25)  // TAODEBUG
      trans
    }
  }

  /**
   * Score the specimen with the given data
   */
  def score(df: DataFrame, measure: Measure): MayFail[Double] = 
    measure % (ensure(df), this)

  def scoreVector(df: DataFrame, measure: MeasureVector): MayFail[Array[Double]] = {
    Console.println("SCORE VECTOR") // TAODEBUG
    Console.println("BEFORE : ", df.columns.mkString(", "))
    Console.println("AFTER  : ", ensure(df).columns.mkString(", "))
    Console.println("OUTPUT COL = ", outputCol)
    measure % (ensure(df), this)
  }
}

/**
 * No machine learning, just wraps the predicted data for further usage
 * NOTE: [[PipelineModel]] is never used in [[DummySpecimen]]
 */
case class DummySpecimen(
  override val featureCol: FeatureColumn,
  override val outputCol: String,
  override val labelCol: String
) extends Specimen {

  override lazy val model = throw new NotImplementedError
  override protected def ensure(df: DataFrame): DataFrame = df // No transformation, no pipeline model
}

case class TrainedSpecimen(
  override val model: PipelineModel,
  override val featureCol: FeatureColumn,
  override val outputCol: String,
  override val labelCol: String
) extends Specimen {
  override def score(df: DataFrame, measure: Measure) = 
    measure match {
      case _:RegressionMeasure => super.score(ensure(df), measure)
      case _                   => Fail(
        s"Unsupported measure type : ${measure.getClass.getName}")
    }
}
