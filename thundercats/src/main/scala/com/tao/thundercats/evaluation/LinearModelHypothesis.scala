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

case class LinearFeatureSelection(
  model: Pipeline,
  override val selector: Select,
  measure: FeatureMeasure
) extends FeatureSelection {
  
  override def run(data: DataFrame, verbose: Boolean=true): MayFail[Outcome] = MayFail {

    select match {
      case DropLeastSignificant(numFeat) => ???
      case TakeMostSignificant(numFeat) => ???
    }

    val models: List[(PipelineModel,Double)] = List.empty
    FeatureSelectionOutcome(models)
  }
}

case class LinearModelSelection(feature: Pipeline, models: Pipeline) extends ModelSelection {
  override def run(data: DataFrame, verbose: Boolean=true): MayFail[Outcome] = ???
}