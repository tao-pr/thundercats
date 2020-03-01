package com.tao.thundercats.model

import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Column,Row}
import org.apache.spark.sql.catalyst.encoders._
import org.apache.spark.sql.{Encoders, Encoder}
import org.apache.spark.sql.avro._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer, VectorAssembler}
import org.apache.spark.ml.{Transformer, PipelineModel}
import org.apache.spark.ml.{Pipeline, Estimator}
import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.ml.param.ParamMap

import com.tao.thundercats.evaluation._

trait CV {
  val grid: ParamMap
  def run(df: DataFrame, pipe: Pipeline): PipelineModel = ???
}

case class SplitCV(override val grid: ParamMap, ratio: Double=0.8) extends CV
case class FoldCV(override val grid: ParamMap, folds: Int=3) extends CV