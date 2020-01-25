package com.tao.thundercats.model

import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Column,Row}
import org.apache.spark.sql.catalyst.encoders._
import org.apache.spark.sql.{Encoders, Encoder}
import org.apache.spark.sql.avro._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.ml.Transformer

import java.io.File
import sys.process._
import scala.reflect.io.Directory
import scala.util.Try

import com.tao.thundercats.physical._
import com.tao.thundercats.functional._

object Feature {
  def evaluate(featureSet: Set[String], pipe: Pipeline, df: DataFrame): MayFail[DataFrame] = ???
}

object Pipe {
  def preset(presetName: String): MayFail[Pipeline] = ???
  def build: MayFail[Pipeline] = ???
  def join(pipes: *Pipeline): MayFail[Pipeline] = ???
  def load(filePath: String): MayFail[Pipeline] = ???
  def save(filePath: String, pipe: Pipeline): MayFail[Pipeline] = ???
}