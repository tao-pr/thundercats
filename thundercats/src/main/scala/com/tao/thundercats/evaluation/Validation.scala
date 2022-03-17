package com.tao.thundercats.evaluation

import com.tao.thundercats.functional._
import com.tao.thundercats.physical._
import org.apache.spark.sql.DataFrame

/**
 * Model validation suite
 */
trait Validation[M <: Measure] {
  val measure: M
  def run(df: DataFrame, design: ModelDesign, feature: FeatureColumn): MayFail[Double]
}

case class CrossValidation[M <: Measure](
  override val measure: M,
  nFolds: Int=3) 
extends Validation[M] {
  override def run(df: DataFrame, design: ModelDesign, feature: FeatureColumn): MayFail[Double] = MayFail {
    Log.info(s"CrossValidation : Running ${nFolds} folds, with measure = ${measure.getClass.getName}")
    val splits = df.randomSplit((1 to nFolds).toArray.map(_ => 1/nFolds.toDouble))
    val folds = (0 until nFolds).map{ i => 
      val dfTrain = splits.zipWithIndex.filter(_._2 != i).map(_._1).reduce(_ union _)
      val dfTest = splits(i)

      // Build the model
      val m = design.toSpecimen(feature, dfTrain)

      // Validate with test set
      Log.info(s"CrossValidation : Scoring fold ${i+1} of ${nFolds}")
      m.score(dfTest, measure).get
    }
    
    folds.sum.toDouble / nFolds.toDouble
  }
}

case class SplitValidation[M <: Measure](
  override val measure: M,
  trainRatio: Float=0.9f) 
extends Validation[M] {
  override def run(df: DataFrame, design: ModelDesign, feature: FeatureColumn): MayFail[Double] = {
    Log.info(s"SplitValidation : ratio of ${trainRatio}, with measure = ${measure.getClass.getName}")
    if (trainRatio <= 0.0 || trainRatio >= 1.0){
      Fail(new AssertionError(
        s"SplitValidation fails with out-of-range training ratio : ${trainRatio}"))
    }
    else MayFail {
      val Array(dfTrain, dfTest) = df.randomSplit(Array(trainRatio, 1-trainRatio))
      
      // Build the model
      val m = design.toSpecimen(feature, dfTrain)
      m.score(dfTest, measure).get
    }
  }
}

