package com.tao.thundercats.evaluation

import java.lang.UnsupportedOperationException

import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Column,Row}
import org.apache.spark.sql.catalyst.encoders._
import org.apache.spark.sql.{Encoders, Encoder}
import org.apache.spark.sql.avro._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.rdd.RDD

import org.apache.spark.ml.feature.{HashingTF, Tokenizer, VectorAssembler}
import org.apache.spark.ml.{Transformer, PipelineModel, CustomPipelineModel}
import org.apache.spark.ml.{Pipeline, Estimator, PipelineStage}
import org.apache.spark.ml.{Predictor}
import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.ml.param._
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.linalg.{VectorUDT, Vector}
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.rdd.DoubleRDDFunctions

import org.apache.spark.mllib.stat.correlation.ExposedPearsonCorrelation
import org.apache.spark.mllib.linalg.{Vector => MLLibVector}

import java.io.File
import java.lang.IllegalArgumentException
import sys.process._
import scala.reflect.io.Directory
import scala.util.Try

import com.tao.thundercats.physical._
import com.tao.thundercats.functional._
import com.tao.thundercats.physical.Implicits._
import com.tao.thundercats.physical.Debugger
import com.tao.thundercats.estimator._
import com.tao.thundercats.evaluation._

/**
 * Trained Model for evaluation
 */
trait Specimen {
  val model: PipelineModel
  val outputCol: String
  val labelCol: String
  val featureCol: FeatureColumn

  /**
   * Ensure the dataframe is transformed before use
   */
  protected def ensure(df: DataFrame): DataFrame = {

    if (df.columns.contains("features") || 
      df.columns.contains(outputCol)){
      Log.debug(s"Specimen : Skipping transformation (${this.getClass.getName})")
      df
    }
    else {
      Log.debug(s"Specimen : Pipeline transformation is operating (${this.getClass.getName})")
      model.transform(df)
    }
  }

  /**
   * Score the specimen with the given data
   */
  def score(df: DataFrame, measure: Measure): MayFail[Double] = 
    measure % (ensure(df), this)

  def scoreVector(df: DataFrame, measure: MeasureVector): MayFail[Array[Double]] = {
    measure % (ensure(df), this)
  }

  /**
   * Score the specimen, as a map (threshold -> score)
   */
  def scoreMap(df: DataFrame, measure: ClassificationMeasure): MayFail[Map[Double,Double]] = 
    measure %% (ensure(df), this)

  /**
   * Generate a cluster from data
   */
  def scoreCluster(df: DataFrame, measure: ClusterMeasure): MayFail[Double] = {
    measure % (ensure(df), this)
  }

  /**
   * Transform the dataframe into feature vectors
   * without running prediction
   */
  def toFeatureVectorRDD(df: DataFrame): RDD[MLLibVector] = {
    val pipeWithoutPredictor = new CustomPipelineModel(model.uid, model.stages.toArray.dropRight(1))
    val featDf = pipeWithoutPredictor.transform(df)
    val featureType = featDf.schema.find(_.name==featureCol).get.dataType
    featDf.rdd.map{ row =>
      featureType match {
        // NOTE: [[VectorType]] is just a public exposure of private [[VectorUDT]]
        case VectorType => 
          // Convert ML vector => MLLIB vector
          val v = row.getAs[org.apache.spark.ml.linalg.Vector](featureCol.colName)
          org.apache.spark.mllib.linalg.Vectors.fromML(v)
        case _ => row.getAs[MLLibVector](featureCol.colName)
      }
    }
  }
}

/**
 * No machine learning, just wraps the predicted data for further usage
 * NOTE: [[PipelineModel]] is never used in [[DummySpecimen]]
 */
case class DummySpecimen(
  override val featureCol: FeatureColumn,
  override val outputCol: String,
  override val labelCol: String
) extends Specimen {

  override lazy val model = throw new NotImplementedError
  override protected def ensure(df: DataFrame): DataFrame = df // No transformation, no pipeline model
}

case class SupervisedSpecimen(
  override val model: PipelineModel,
  override val featureCol: FeatureColumn,
  override val outputCol: String,
  override val labelCol: String
) extends Specimen {
  override def score(df: DataFrame, measure: Measure) = {
    measure match {
      case _:RegressionMeasure => super.score(ensure(df), measure)
      case _:ClassificationMeasure => super.score(ensure(df), measure)
      case _ => Fail(new UnsupportedOperationException(
        s"Unsupported measure type for Supervised specimen: ${measure.className}"))
    }
  }
}

case class UnsupervisedSpecimen(
  override val model: PipelineModel,
  override val featureCol: FeatureColumn,
  override val outputCol: String
) extends Specimen {
  override val labelCol = ""
  override def score(df: DataFrame, measure: Measure) = {
    measure match {
      case _: ClusterMeasure => super.score(ensure(df), measure)
      case _ => Fail(new UnsupportedOperationException(
        s"Unsupported measure type for Unsupervised specimen : ${measure.className}"))
    }
  }
}
