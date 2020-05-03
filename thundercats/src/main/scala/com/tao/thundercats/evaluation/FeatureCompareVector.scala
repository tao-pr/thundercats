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

/**
 * Individual feature comparison (as vector of all columns)
 */
trait FeatureCompareVector[A <: MeasureVector] extends BaseCompare[A] {
  override val measure: A
  override def bestOf(design: ModelDesign, comb: Iterable[FeatureColumn], df: DataFrame): Option[(Double, Specimen)] = {
    ???
    // TAOTODO:
  }
}