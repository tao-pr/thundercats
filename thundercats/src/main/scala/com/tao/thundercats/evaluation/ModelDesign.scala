package com.tao.thundercats.evaluation

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
import org.apache.spark.ml.{Predictor}
import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.ml.param._
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.mllib.stat.correlation.ExposedPearsonCorrelation
import org.apache.spark.rdd.DoubleRDDFunctions

import java.io.File
import java.lang.IllegalArgumentException
import sys.process._
import scala.reflect.io.Directory
import scala.util.Try

import com.tao.thundercats.physical._
import com.tao.thundercats.functional._
import com.tao.thundercats.physical.Implicits._
import com.tao.thundercats.estimator._

/**
 * Draft model for training
 * can be trained multiple times with different feature columns
 */
trait ModelDesign {
  val outputCol: String
  val labelCol: String
  def toSpecimen(feature: FeatureColumn, df: DataFrame): Specimen
}

/**
 * [[DummyModelDesign]] does not train any pipeline
 */
case class DummyModelDesign(override val labelCol: String) 
extends ModelDesign {
  override val outputCol = ""
  override def toSpecimen(feature: FeatureColumn, df: DataFrame) = 
    // NOTE: Specified feature col will be used as direct output 
    DummySpecimen(feature, labelCol, feature.colName)
}

/**
 * Model prototype of any kind
 */
case class FeatureModelDesign(
  override val outputCol: String, 
  override val labelCol: String,
  estimator: Pipeline,
  featurePipe: Option[PipelineStage] = None)
extends ModelDesign {
  
  private def printStages(pipeline: Pipeline): Unit = {
    pipeline.getStages.foreach{stage =>
      if (stage.isInstanceOf[Pipeline]){
        printStages(stage.asInstanceOf[Pipeline])
      }
      else Console.println(s"... ${stage.getClass.getName}")
    }
  }

  override def toSpecimen(feature: FeatureColumn, df: DataFrame) = {
    var pipe = feature % (estimator, featurePipe)
    Log.info(s"Fitting FeatureModel: labelCol=${labelCol}, outputCol=${outputCol}")

    // TAODEBUG
    Console.println("PIPELINE STRUCRTURE")
    printStages(pipe)

    val fitted = pipe.fit(df)
    TrainedSpecimen(fitted, feature, outputCol, labelCol)
  }
}
