package com.tao.thundercats.evaluation

import com.tao.thundercats.physical._
import org.apache.spark.sql.DataFrame

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
