package com.tao.thundercats.model

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
import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.ml.param._

import java.io.File
import sys.process._
import scala.reflect.io.Directory
import scala.util.Try

import com.tao.thundercats.physical._
import com.tao.thundercats.functional._
import com.tao.thundercats.physical.Implicits._
import com.tao.thundercats.estimator._

trait Significance extends MayFail[Significance] {
  def by(metric: Metric): Significance
  def > (another: Significance): Boolean
}

/**
 * Failure of significance test or operation
 */
case class FailSig(errorMessage: String) extends Significance with MayFail[Significance] {
  override def map(f: Significance => Significance): MayFail[Significance] = this
  override def flatMap(g: Significance => MayFail[Significance]): MayFail[Significance] = this
  override def get: Significance = throw new java.util.NoSuchElementException("No value resolved")
  override def isFailing = true
  override def getError: Option[String] = Some(errorMessage)

  override def by(metric: Metric): Significance = this
  override def > (another: Significance): Boolean = false
}

trait ModelSignificance extends Significance
trait FeatureSignificance extends Significance { val featureCol: String }

object Significance {
  def apply(modelPipe: Pipeline): Significance = {
    assert(modelPipe.getStages.size >= 1)
    ???
  }
}