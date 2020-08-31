package com.tao.thundercats.evaluation

import org.apache.spark.sql.{Dataset, DataFrame}

import com.tao.thundercats.physical._
import com.tao.thundercats.functional._
import com.tao.thundercats.physical.Implicits._
import com.tao.thundercats.estimator._
import com.tao.thundercats.evaluation._

trait FeatureSelector {
  def selectSubset(df: DataFrame, model: ModelDesign, features: Iterable[FeatureColumn]): Iterable[(Double, FeatureColumn)]
}

private [evaluation] trait Significance { val zReject: Double }
case object AllSignificance extends Significance { override val zReject = Double.MinValue }
case object Significance50p extends Significance { override val zReject = 0.674 }
case object Significance80p extends Significance { override val zReject = 1.282 }
case object Significance90p extends Significance { override val zReject = 1.645 }
case object Significance95p extends Significance { override val zReject = 1.960 }
case object Significance98p extends Significance { override val zReject = 2.326 }
case object Significance99p extends Significance { override val zReject = 2.576 }

/**
 * Selecting best features by significance (z-score)
 */
case class ZScoreFeatureSelector(sig: Significance) extends FeatureSelector {
  def selectSubset(df: DataFrame, model: ModelDesign, features: Iterable[FeatureColumn]) = {
    // Measure z-scores of each feature individually
    val zscore = ZScore
    val cvector = RegressionFeatureCompareVector(zscore)
    val (scores, _) = cvector.allOf(model, features, df)

    // Select most significant features
    Log.info(s"[ZScoreFeatureSelector] rejecting features with zscore below ${sig.zReject}")
    val significantFeatures = scores.map{ case (z,c) =>
      if (z >= sig.zReject){
        Log.info(f"[ZScoreFeatureSelector] accepting ${c} (zscore of ${z}%.3f)")
        Some((z, Feature(c)))
      }
      else {
        Log.info(f"[ZScoreFeatureSelector] rejecting ${c} (zscore of ${z}%.3f)")
        None
      }
    }.flatten

    significantFeatures
  } 
}

/**
 * Selecting best N features
 */
case class BestNFeaturesSelector(top: Int, measure: BaseMeasure[_]) extends FeatureSelector {
  def selectSubset(df: DataFrame, model: ModelDesign, features: Iterable[FeatureColumn]) = {
    measure match {
      case v:MeasureVector => selectSubsetFromVector(df, model, features, v)
      case m:Measure => selectSubsetFromScalar(df, model, features, m)
    }
  }

  private def selectSubsetFromScalar(df: DataFrame, model: ModelDesign, features: Iterable[FeatureColumn], m: Measure) = {
    val cvector = new DummyFeatureCompare(m)
    val scores = cvector.allOf(model, features, df)

    Log.info(s"[BestNFeaturesSelector] selecting ${top} features out of ${features.size}")
    val sorted = scores.toList.sortWith{ case(a,b) => m.isBetter(a._1, b._1) }

    sorted.take(top).map{ case (score, specimen) => 
      (score, specimen.featureCol) 
    }
  }

  private def selectSubsetFromVector(df: DataFrame, model: ModelDesign, features: Iterable[FeatureColumn], m: MeasureVector) = {
    val cvector = DummyFeatureCompareVector(m)
    val (scores, _) = cvector.allOf(model, features, df)

    Log.info(s"[BestNFeaturesSelector] selecting ${top} features out of ${features.size}")
    val sorted = scores.sortBy(-_._1) // Greater score is better

    sorted.take(top).map{ case (score, feat) => 
      (score, Feature(feat))
    }
  }
}