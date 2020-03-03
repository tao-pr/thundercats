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


/**
 * Collection of basic transformer generators
 */
object Features {

  /**
   * Create a pipeline which tokenises and encode text
   * as an array of double
   */
  def encodeStrings(
    df: DataFrame,
    encoder: EncoderMethod = Murmur,
    tokeniser: TokenMethod = WhiteSpaceToken,
    suffix: String = "",
    ignoreColumns: Set[String]=Set.empty): PipelineStage = {
    val blocks = df
      .schema
      .toList.collect{ 
        case StructField(colName,StringType,_,_) if !ignoreColumns.contains(colName) => 
          new StringEncoder(encoder, tokeniser)
            .setInputCol(colName)
            .setOutputCol(colName + suffix)
      }

    new Pipeline().setStages(blocks.toArray)
  }

  /**
   * Scale all numbers in the specified columns
   * so they have zero mean and unit variance
   */
  def standardiseNumbers(
    df: DataFrame,
    suffix: String = "",
    ignoreColumns: Set[String]=Set.empty
  ): PipelineStage = {
    val blocks = df
      .schema
      .toList.collect{
        case StructField(colName,DoubleType,_,_) if !ignoreColumns.contains(colName) =>
          new StandardScaler().setInputCol(colName)
                              .setOutputCol(colName + suffix)
    }

    new Pipeline().setStages(blocks.toArray)
  }

  /**
   * Create a pipeline which scales or normalises the numbers 
   */
  def scaleNumbers(
    df: DataFrame,
    normalised: Boolean = true,
    logScale: Boolean=false, 
    suffix: String = "",
    ignoreColumns: Set[String]=Set.empty): PipelineStage = {
    val blocks = df
      .schema
      .toList.collect{
        case StructField(colName,DoubleType,_,_) if !ignoreColumns.contains(colName) =>
          new Scaler().setInputCol(colName)
                      .setOutputCol(colName + suffix)
                      .setLogScale(logScale)
                      .setNorm(normalised)
      }

    new Pipeline().setStages(blocks.toArray)
  }

  def vectorise(df: DataFrame, ignoreColumns: Set[String]): PipelineStage = {
    val columns = df
      .schema
      .toList
      .sortBy(_.name)
      .filterNot { case StructField(name,_,_,_) => ignoreColumns contains name }
      .collect {
        case StructField(p,DoubleType,_,_) => p
        case StructField(p,IntegerType,_,_) => p
        case StructField(p,FloatType,_,_) => p
        case StructField(p,ArrayType(DoubleType,_),_,_) => p
        case StructField(p,ArrayType(FloatType,_),_,_) => p
        case StructField(p,ArrayType(IntegerType,_),_,_) => p
      }.toArray

    new VectorAssembler().setInputCols(columns).setOutputCol("features")
  }
}



