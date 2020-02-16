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

/**
 * Collection of basic transformer generators
 */
object Feature {

  def encodeStrings(df: DataFrame, ignoreColumns: Set[String]=Set.empty): PipelineStage = {
    val blocks = df
      .schema
      .toList.collect{ 
        case StructField(colName,StringType,_,_) if !ignoreColumns.contains(colName) => 
          new HashingTF().setInputCol(colName).setOutputCol(colName)
      }

    new Pipeline().setStages(blocks.toArray)
  }

  def scaleNumbers(df: DataFrame, byNorms: Option[Double]=Some(1.0), logScale: Boolean=false, ignoreColumns: Set[String]=Set.empty): PipelineStage = {
    val blocks = df
      .schema
      .toList.collect{
        case StructField(colName,DoubleType,_,_) if !ignoreColumns.contains(colName) =>
          new Scaler().setInputCol(colName)
                      .setOutputCol(colName)
                      .setLogScale(logScale)
                      .setP(byNorms.getOrElse(0.0))
      }

    new Pipeline().setStages(blocks.toArray)
  }
}


