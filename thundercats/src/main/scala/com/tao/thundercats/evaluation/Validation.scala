package com.tao.thundercats.evaluation

import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Column,Row}
import org.apache.spark.sql.catalyst.encoders._
import org.apache.spark.sql.{Encoders, Encoder}
import org.apache.spark.sql.avro._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import org.apache.spark.ml.{Transformer, PipelineModel}
import org.apache.spark.ml.{Pipeline, Estimator, PipelineStage}
import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.ml.param._

import breeze.linalg.DenseVector

import com.tao.thundercats.physical._
import com.tao.thundercats.functional._
import com.tao.thundercats.physical.Implicits._
import com.tao.thundercats.estimator._

/**
 * Model validation suite
 */
trait Validation[M <: Measure, D <: ModelDesign] {
  val measure: M
  def run(df: DataFrame, design: D, feature: Feature): MayFail[Double]
}

case class CrossValidation[M <: Measure, D <: ModelDesign](
  override val measure: M,
  nFolds: Int=3) 
extends Validation[M,D] {
  override def run(df: DataFrame, design: D, feature: Feature): MayFail[Double] = MayFail {
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

case class SplitValidation[M <: Measure, D <: ModelDesign](
  override val measure: M,
  trainRatio: Float=0.9f) 
extends Validation[M,D] {
  override def run(df: DataFrame, design: D, feature: Feature): MayFail[Double] = MayFail {
    ???
  }
}

