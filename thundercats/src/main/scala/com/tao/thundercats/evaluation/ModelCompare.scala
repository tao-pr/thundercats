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
 * Choosing the best model 
 * from Iterable[ModelDesign] => (Specimen)
 */
trait ModelCompare[A <: Measure] {
  val measure: A
  val feature: FeatureColumn

  def bestOf(df: DataFrame, models: Iterable[ModelDesign]): Option[(Double, Specimen)] = {
    val takeBetterModel = (a: (Double, Specimen), b: (Double, Specimen)) => {
      val (bestScore, bestSpecimen) = a
      val (anotherScore, anotherSpecimen) = b
      if (measure.isBetter(bestScore, anotherScore)) a
      else b
    }
    allOf(df, models).reduceLeftOption(takeBetterModel)
  }

  def allOf(df: DataFrame, models: Iterable[ModelDesign]): Iterable[(Double, Specimen)] = {
    models.map{ design =>
      val specimen = design.toSpecimen(feature, df)
      val scoreOpt = specimen.score(df, measure)

      scoreOpt.mapOpt{ score => 
        Log.info(s"[${this.getClass.getName}] ${measure.className} score : ${feature.colName} = ${score}")
        (score, specimen)
      }
    }.flatten
  }
}

class RegressionModelCompare[A <: RegressionMeasure](
  override val measure: A,
  override val feature: FeatureColumn) 
extends ModelCompare[A]

class ClassificationModelCompare[A <: ClassificationMeasure](
  override val measure: A,
  override val feature: FeatureColumn)
extends ModelCompare[A]

class ClusterModelCompare[A <: ClusterMeasure](
  override val measure: A,
  override val feature: FeatureColumn)
extends ModelCompare[A]
