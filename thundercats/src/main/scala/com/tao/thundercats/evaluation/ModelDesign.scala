package com.tao.thundercats.evaluation

import com.tao.thundercats.physical._
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.sql.DataFrame

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

case class SupervisedModelDesign(
  override val outputCol: String, 
  override val labelCol: String,
  estimator: Pipeline,
  featurePipe: Option[PipelineStage] = None)
extends ModelDesign {

  override def toSpecimen(feature: FeatureColumn, df: DataFrame) = {
    var pipe = feature % (estimator, featurePipe)
    Log.info(s"Fitting Supervised Model: labelCol=${labelCol}, outputCol=${outputCol}")
    val fitted = pipe.fit(df)
    val modelClass = Debugger.modelToString(fitted)
    Log.info(s"Fitted Supervised Model: ${modelClass}")
    SupervisedSpecimen(fitted, feature, outputCol, labelCol)
  }
}

case class UnsupervisedModelDesign(
  override val outputCol: String,
  estimator: Pipeline,
  featurePipe: Option[PipelineStage] = None)
extends ModelDesign {

  override val labelCol = ""

  override def toSpecimen(feature: FeatureColumn, df: DataFrame) = {
    var pipe = feature % (estimator, featurePipe)
    Log.info(s"Fitting Unsupervised Model: outputCol=${outputCol}")
    val fitted = pipe.fit(df)
    val modelClass = Debugger.modelToString(fitted)
    Log.info(s"Fitted Unsupervised Model: ${modelClass}")
    UnsupervisedSpecimen(fitted, feature, outputCol)
  }
}
