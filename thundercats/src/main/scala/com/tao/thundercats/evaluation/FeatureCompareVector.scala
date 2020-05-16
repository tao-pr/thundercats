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

  def allOf(design: ModelDesign, comb: Iterable[FeatureColumn], df: DataFrame): (Array[(Double,String)], Specimen) = {
    val features = AssemblyFeature.fromIterable(comb)
    val specimen = design.toSpecimen(features, df)
    val scoreVectorOpt = measure % (df, specimen)

    assert(features.size == scoreVectorOpt.size)

    val zippedScore = scoreVectorOpt(_.getOrElse(Double.MinValue)).zip(features)

    (zippedScore, specimen)
  }
  
  override def bestOf(design: ModelDesign, comb: Iterable[FeatureColumn], df: DataFrame): Option[(Double, Specimen)] = {
    
    // Calculate scores of all columns individually
    // and locate the best
    val (zippedScore, specimen) = allOf(design, comb, df)
    val best = zippedScore.max

    // TAOTODO: Redesign this, it should return "string" column, not specimen
  }
}