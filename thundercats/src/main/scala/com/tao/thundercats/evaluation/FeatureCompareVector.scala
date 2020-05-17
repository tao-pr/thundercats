package com.tao.thundercats.evaluation

import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Column,Row}
import org.apache.spark.sql.catalyst.encoders._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import com.tao.thundercats.physical._
import com.tao.thundercats.functional._
import com.tao.thundercats.physical.Implicits._
import com.tao.thundercats.estimator._
import com.tao.thundercats.evaluation._

/**
 * Individual feature comparison (as vector of all columns)
 */
trait FeatureCompareVector[A <: MeasureVector] extends BaseCompare[A] {
  override val measure: A

  def allOf(design: ModelDesign, comb: Iterable[FeatureColumn], df: DataFrame): (Array[(Double, String)], Specimen) = {
    val features = AssemblyFeature.fromIterable(comb)
    val specimen = design.toSpecimen(features, df)
    val scoreVectorOpt = measure % (df, specimen)

    scoreVectorOpt.map{ scoreVector =>
      assert(features.asArray.size == scoreVector.size)
      val zippedScore = scoreVector.zip(features.asArray)
      (zippedScore,specimen)

    }.getOrElse((Array.empty,specimen))
  }

  protected def findBest(zippedScore: Array[(Double, String)]) = zippedScore.max
  
  override def bestOf(design: ModelDesign, comb: Iterable[FeatureColumn], df: DataFrame): Option[(Double, FeatureColumn, Specimen)] = {
    
    // Calculate scores of all columns individually
    // and locate the best
    val (zippedScore, specimen) = allOf(design, comb, df)
    if (zippedScore.isEmpty) None
    else {
      val (bestScore, bestFeat) = findBest(zippedScore)
      Some((bestScore, Feature(bestFeat), specimen))
    }
  }
}

case class DummyFeatureCompareVector(override val measure: MeasureVector)
extends FeatureCompareVector[MeasureVector]

case class RegressionFeatureCompareVector(override val measure: RegressionMeasureVector)
extends FeatureCompareVector[RegressionMeasureVector]



