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
   * Create a pipeline for training
   */
  def %(estimator: Pipeline): Pipeline
}

case class Feature(c: String) extends FeatureColumn {
  override def %(estimator: Pipeline) = estimator
}

case class AssemblyFeature(cs: Seq[String], outputCol: String="features") 
extends FeatureColumn {
  override def %(estimator: Pipeline) = {
    val vecAsm = new VectorAssembler()
      .setInputCols(cs.toArray)
      .setOutputCol(outputCol)
    new Pipeline().setStages(Array(vecAsm, estimator))
  }
}

trait FeatureCompare[A <: Measure] {
  lazy val blueprint: Blueprint
  lazy val measure: A
  def bestOf(comb: Iterable[FeatureColumn], df: DataFrame): Option[(Double, Specimen)] = {
    // Find the best features out of the bound specimen
    val measures = comb.map{ c => 
      // Train the model (specimen) by given column(s)
      val specimen = blueprint.toSpecimen(c, df)
      val score = specimen.score(df, measure).get
      (score, c)
    }
    // Identify the best specimen
    measures.reduceLeftOption{ case(a,b) => 
      val (bestScore, bestSpecimen) = a
      val (anotherScore, anotherSpecimen) = b
      if (m.isBetter(bestScore, anotherScore)) a
      else b
    }
  }
}

/**
 * Compare features for linear regression model
 */
class RegressionFeatureCompare[A <: RegressionMeasure](m: A)
extends FeatureCompare[A] {
  override def bestOf(comb: Iterable[FeatureColumn]) = ???
}

/**
 * Non-binding feature comparison, supports any feature types
 */
class DummyFeatureCompare(m: Measure)
extends FeatureCompare[Measure] {
  override def bestOf(comb: Iterable[FeatureColumn]) = {
    ???
  }
}