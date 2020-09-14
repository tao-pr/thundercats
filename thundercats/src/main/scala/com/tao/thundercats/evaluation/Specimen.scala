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
import com.tao.thundercats.evaluation._

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
    // Remove existing output / features columns
    val dfFree = Seq("features", outputCol).foldLeft(df){ case(df_,c) =>
      if (df_.columns contains c) 
        df_.drop(c)
      else df_
    }
    // TAODEBUG
    printStages(model)

    model.transform(dfFree)
  }

  private def printStages(model: PipelineModel): Unit = {
    model.stages.foreach{ trans =>
      if (trans.isInstanceOf[PipelineModel])
        printStages(trans.asInstanceOf[PipelineModel])
      else 
        Console.println(s"... ${trans.getClass.getName}")
    }
  }

  /**
   * Score the specimen with the given data
   */
  def score(df: DataFrame, measure: Measure): MayFail[Double] = 
    measure % (ensure(df), this)

  def scoreVector(df: DataFrame, measure: MeasureVector): MayFail[Array[Double]] = {
    measure % (ensure(df), this)
  }

  /**
   * Score the specimen, as a map (threshold -> score)
   */
  def scoreMap(df: DataFrame, measure: Measure): MayFail[Map[Double,Double]] = 
    measure match {
      case m:ClassificationMeasure => m %% (ensure(df), this)
      case _ => Fail(f"${measure.className} does not support scoreMap")
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
      case _:ClassificationMeasure => super.score(ensure(df), measure)
      case _                   => Fail(
        s"Unsupported measure type : ${measure.className}")
    }
}
