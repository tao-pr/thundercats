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
import org.apache.spark.ml.{Pipeline, Estimator}

import java.io.File
import sys.process._
import scala.reflect.io.Directory
import scala.util.Try

import com.tao.thundercats.physical._
import com.tao.thundercats.functional._
import com.tao.thundercats.physical.Implicits._

object Feature {

  /** 
   * DataFrame with tagged columns
   */
  case class TaggedDataFrame(
    df: DataFrame,
    featureCols: Set[Column], 
    targetCol: Column, 
    labelCol: Column)

  /**
   * All non-labeled numerical columns, or array of numerical columns 
   * will become features
   */
  def fromDF(df: DataFrame, targetCol: String, labelCol: String): MayFail[TaggedDataFrame] = MayFail {
    val featureCols = df.columns.toSet -- Set(targetCol, labelCol)
    val schemaMap = df.schemaMap
    val numFeatureCols = featureCols
      .map{ c => schemaMap(c) match {
        case IntegerType => Some(col(c))
        case DoubleType => Some(col(c))
        case FloatType => Some(col(c))
        case ArrayType(IntegerType,_) => Some(col(c))
        case ArrayType(DoubleType,_) => Some(col(c))
        case ArrayType(FloatType,_) => Some(col(c))
        case _ => None
      }}.flatten
    
    TaggedDataFrame(df, numFeatureCols, col(targetCol), col(labelCol))
  }

  def evaluate(pipe: Pipeline, taggedDf: TaggedDataFrame, output: AnyVal): MayFail[TaggedDataFrame] = ???
  def selection(pipe: Pipeline, taggedDf: TaggedDataFrame, top: Int=10, metric: AnyVal): MayFail[TaggedDataFrame] = ???
}

object Pipe {
  def join(pipes: Pipeline*): MayFail[Pipeline] = MayFail {
    new Pipeline().setStages(pipes.toArray)
  }

  def load(filePath: String): MayFail[Pipeline] = MayFail {
    Pipeline.load(filePath)
  }

  def save(filePath: String, pipe: Pipeline): MayFail[Pipeline] = MayFail {
    pipe.save(filePath)
    pipe
  }

  def getEstimator(pipe: Pipeline): MayFail[Pipeline] = MayFail {
    pipe.getStages.collect{ case p: Estimator[_] => new Pipeline().setStages(Array(p)) }.last
  }

  def withoutEstimator(pipe: Pipeline): MayFail[Pipeline] = MayFail {
    new Pipeline().setStages(pipe.getStages.collect{ case t: Transformer => t })
  }
}