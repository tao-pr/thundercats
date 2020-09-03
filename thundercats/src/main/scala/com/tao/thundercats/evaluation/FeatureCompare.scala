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
  def colName: String
  def asArray: Array[String]
  def size: Int
}

case class Feature(c: String) extends FeatureColumn {
  override def %(estimator: Pipeline) = {
    val vecAsm = new VectorAssembler()
      .setInputCols(Array(c))
      .setOutputCol("features")
    new Pipeline().setStages(Array(vecAsm, estimator))
  }
  override def colName = c
  override def asArray = Array(c)
  override def size = 1
}

case class AssemblyFeature(cs: Seq[String], asVectorCol: String="features") 
extends FeatureColumn {
  override def %(estimator: Pipeline) = {
    val vecAsm = new VectorAssembler()
      .setInputCols(cs.toArray)
      .setOutputCol(asVectorCol)
    // NOTE: Presume [[estimator]] takes a vector input [[asVectorCol]]
    new Pipeline().setStages(Array(vecAsm, estimator))
  }
  override def colName = asVectorCol
  override def asArray = cs.toArray
  override def size = cs.size
}

object AssemblyFeature {
  def fromIterable(arr: Iterable[FeatureColumn]): AssemblyFeature = {
    AssemblyFeature(arr.map(_.asArray).flatten.toSeq)
  }
}

trait BaseCompare[A <: BaseMeasure[_]] {
  val measure: A

  /**
   * Get the best feature and best specimen
   */
  def bestOf(
    design: ModelDesign, 
    comb: Iterable[FeatureColumn],
    df: DataFrame): Option[(Double, FeatureColumn, Specimen)]
}

/**
 * Feature comparison on whole model level
 */
trait FeatureCompare[A <: Measure] extends BaseCompare[A] {
  override val measure: A

  protected def bestMeasures(measures: Iterable[(Double,Specimen)]): Option[(Double, Specimen)] = {
    val takeBetterScore = (a: (Double, Specimen), b: (Double, Specimen)) => {
      val (bestScore, bestSpecimen) = a
      val (anotherScore, anotherSpecimen) = b
      if (measure.isBetter(bestScore, anotherScore)) a
      else b
    }
    measures.reduceLeftOption(takeBetterScore)
  }

  def allOf(design: ModelDesign, comb: Iterable[FeatureColumn], df: DataFrame): Iterable[(Double, Specimen)] = {
    Log.info(s"[FeatureCompare] allOf : ${comb.map(_.colName).mkString(", ")}")
    // Find the best features out of the bound specimen
    val measures: Iterable[(Double,Specimen)] = comb.map{ c => 
      // Train the model (specimen) by given column(s)
      val specimen = design.toSpecimen(c, df)
      val scoreOpt = specimen.score(df, measure)

      scoreOpt.mapOpt{ score => 
        Log.info(s"[FeatureCompare] ${measure.className} score : ${c.colName} = ${score}")
        (score, specimen)
      }
    }.flatten

    measures
  }

  override def bestOf(design: ModelDesign, comb: Iterable[FeatureColumn], df: DataFrame): Option[(Double, FeatureColumn, Specimen)] = {
    bestMeasures(allOf(design, comb, df)).map{ case (bestScore, specimen) => 
      // Just take feature column from the specimen
      // It's already the only feature we use
      val bestFeat = specimen.featureCol
      Log.info(s"[FeatureCompare] bestOf : identifying ${bestFeat} as the best feature (score = ${bestScore})")
      (bestScore, bestFeat, specimen)
    }
  }
}

/**
 * No-trained model, just simply presume the data has all candidate prediction columns
 */
class DummyFeatureCompare(override val measure: Measure)
extends FeatureCompare[Measure]

class RegressionFeatureCompare(override val measure: RegressionMeasure)
extends FeatureCompare[RegressionMeasure]

