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
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.Pipeline

import java.io.File
import sys.process._
import scala.reflect.io.Directory
import scala.util.Try

import com.tao.thundercats.physical._
import com.tao.thundercats.functional._

object Feature {

  case class TaggedDataFrame(
    df: DataFrame,
    featureCols: Set[Column], 
    targetCol: Column, 
    labelCol: Column)

  def fromDF(df: DataFrame, targetCol: Column, labelCol: Column): MayFail[TaggedDataFrame] = {
    val featureCols = df.columns.toSet -- Set(targetCol, labelCol)

    ???
  }

  def evaluate(pipe: Pipeline, taggedDf: TaggedDataFrame): MayFail[TaggedDataFrame] = ???
  def selection(pipe: Pipeline, taggedDf: TaggedDataFrame, top: Int=10): MayFail[TaggedDataFrame] = ???
}

object Pipe {
  def join(pipes: Pipeline*): MayFail[Pipeline] = ???
  def load(filePath: String): MayFail[Pipeline] = ???
  def save(filePath: String, pipe: Pipeline): MayFail[Pipeline] = ???
}