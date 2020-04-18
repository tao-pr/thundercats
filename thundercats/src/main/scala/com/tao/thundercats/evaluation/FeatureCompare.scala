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
  val blueprint: Blueprint
  val measure: A
  def bestOf(comb: Iterable[FeatureColumn], df: DataFrame): Option[(Double, Specimen)] = {
    // Find the best features out of the bound specimen
    val measures: Iterable[(Double,Specimen)] = comb.map{ c => 
      // Train the model (specimen) by given column(s)
      val specimen = blueprint.toSpecimen(c, df)
      specimen.mapOpt{s: Specimen => 
        val score = s.score(df, measure).get
        (score, s)
      }
    }.flatten

    val takeBetterScore = (a: (Double,Specimen), b: (Double, Specimen)) => {
      val (bestScore, bestSpecimen) = a
      val (anotherScore, anotherSpecimen) = b
      if (measure.isBetter(bestScore, anotherScore)) a
      else b
    }

    measures.reduceLeftOption(takeBetterScore)
  }
}

/**
 * Compare features for linear regression model
 */
class RegressionFeatureCompare[A <: RegressionMeasure](
  override val measure: A, 
  override val blueprint: Blueprint)
extends FeatureCompare[A] 

/**
 * Non-binding feature comparison, supports any feature types
 */
class DummyFeatureCompare(override val measure: Measure)
extends FeatureCompare[Measure] {
  val blueprint: Blueprint = throw new NotImplementedError
}