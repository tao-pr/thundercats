package com.tao.thundercats.evaluation

import com.tao.thundercats.physical._
import org.apache.spark.sql.DataFrame

/**
 * Individual feature comparison (as vector of all columns)
 */
trait FeatureCompareVector[A <: MeasureVector] extends BaseCompare[A] {
  override val measure: A

  def allOf(design: ModelDesign, comb: Iterable[FeatureColumn], df: DataFrame): (Array[(Double, String)], Specimen) = {
    Log.info(s"[FeatureCompareVector] allOf : ${comb.map(_.colName).mkString(", ")}")
    val features = AssemblyFeature.fromIterable(comb)
    val specimen = design.toSpecimen(features, df)
    val scoreVectorOpt = specimen.scoreVector(df, measure)

    scoreVectorOpt.map{ scoreVector =>
      assert(features.asArray.size == scoreVector.size)
      val zippedScore = scoreVector.zip(features.asArray)

      Log.info(s"[FeatureCompareVector] ${measure.className} score vector : [${zippedScore.map{ case (s,c) => s"${c} = ${s}" }.mkString(", ")}]")
      (zippedScore,specimen)

    }.getOrElse((Array.empty,specimen))
  }

  protected def findBest(zippedScore: Array[(Double, String)]) = zippedScore.max
  
  override def bestOf(design: ModelDesign, comb: Iterable[FeatureColumn], df: DataFrame): Option[(Double, FeatureColumn, Specimen)] = {
    
    // Calculate scores of all columns individually
    // and locate the best
    val (zippedScore, specimen) = allOf(design, comb, df)
    if (zippedScore.isEmpty) {
      Log.info(s"[FeatureCompareVector] bestOf ${measure.className} No scores to choose from")
      None
    }
    else {
      val (bestScore, bestFeat) = findBest(zippedScore)
      Log.info(s"[FeatureCompareVector] bestOf ${measure.className} : identifying ${bestFeat} as best feature (score = ${bestScore})")
      Some((bestScore, Feature(bestFeat), specimen))
    }
  }
}

case class DummyFeatureCompareVector(override val measure: MeasureVector)
extends FeatureCompareVector[MeasureVector]

case class RegressionFeatureCompareVector(override val measure: RegressionMeasureVector)
extends FeatureCompareVector[RegressionMeasureVector]



