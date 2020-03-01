package com.tao.thundercats.evaluation

import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Column,Row}
import org.apache.spark.sql.catalyst.encoders._
import org.apache.spark.sql.{Encoders, Encoder}
import org.apache.spark.sql.avro._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import org.apache.spark.ml.feature._
import org.apache.spark.ml.{Transformer, PipelineModel}
import org.apache.spark.ml.{Pipeline, Estimator, PipelineStage}
import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.ml.Model

import java.io.File
import sys.process._
import scala.reflect.io.Directory
import scala.util.Try

import com.tao.thundercats.physical._
import com.tao.thundercats.functional._
import com.tao.thundercats.physical.Implicits._
import com.tao.thundercats.model.{CV, Pipe}

/**
 * Base hypothesis tester
 */
trait Hypothesis {
  def run(data: DataFrame, verbose: Boolean=true): MayFail[Outcome]
}

/**
 * Base hypothesis test outcome
 */
trait Outcome {
  def getBestModels: List[PipelineModel]
}


private [evaluation] trait Select
case class DropLeastSignificant(num: Int) extends Select
case class TakeMostSignificant(num: Int) extends Select

trait FeatureSelection extends Hypothesis { val selector: Select }
trait ModelSelection extends Hypothesis

case class FeatureSelectionOutcome(models: List[(PipelineModel,Double)]) extends Outcome {
  override def getBestModels: List[PipelineModel] = models.map(_._1)
}

case class ModelSelectionOutcome(model: PipelineModel) extends Outcome {
  override def getBestModels: List[PipelineModel] = List(model)
}
