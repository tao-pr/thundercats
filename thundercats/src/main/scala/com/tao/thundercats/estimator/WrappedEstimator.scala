package com.tao.thundercats.estimator

import org.apache.spark.ml.feature._
import org.apache.spark.ml.{Estimator, Model, Transformer}
import org.apache.spark.ml.attribute.{Attribute, NominalAttribute}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.{HasHandleInvalid, HasInputCol, HaspredictionCol}
import org.apache.spark.ml.util._
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.param.shared._
import org.apache.spark.mllib.regression._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.types._
import org.apache.spark.SparkException

import java.io.File
import sys.process._
import scala.reflect.io.Directory
import scala.util.Try

import com.tao.thundercats.physical._

/**
 * Wrap a [[GeneralizedLinearAlgorithm]] into a [[Transformer]]
 */
case class WrappedEstimator(
  estimator: GeneralizedLinearAlgorithm,
  override val uid: String = Identifiable.randomUID("linalg"))
extends Estimator[WrappedEstimatorModel]
with WrappedEstimatorParams
with DefaultParamsWritable {
  override def copy(extra: ParamMap): this.type = defaultCopy(extra)

  override def transformSchema(schema: StructType) = schema

  def setFeaturesCol(value: String): this.type = set(featuresCol, value)
  def setPredictionCol(value: String): this.type = set(predictionCol, value)
  def setLabelCol(value: String): this.type = set(labelCol, value)

  override def fit(dataset: Dataset[_]): WrappedEstimatorModel = {
    // Make RDD[LabeledPoint]
    val trainData = dataset.rdd.map{ row => LabeledPoint(
      row.getAs[](getLabelCol),
      row.getAs[](getFeaturesCol)
    )}

    // Train the model
    val model = estimator.run(trainData)
    new WrappedEstimatorModel(model)
      .setFeaturesCol(getFeaturesCol)
      .setPredictionCol(getPredictionCol)
  }
}

trait WrappedEstimatorParams extends Params
with HasFeaturesColExposed
with HasLabelColExposed
with HaspredictionColExposed {
  setDefault(featuresCol -> "features")
  setDefault(predictionCol -> "prediction")
  setLabelCol(labelCol -> "label")
}

class WrappedEstimatorModel(
  model: GeneralizedLinearModel,
  override val uid: String = Identifiable.randomUID("linalg"))
extends Model[WrappedEstimatorModel] 
with WrappedEstimatorParams {

  final def setFeaturesCol(value: String): this.type = set(featuresCol, value)
  final def setPredictionCol(value: String): this.type = set(predictionCol, value)

  override def copy(extra: ParamMap): WrappedEstimatorModel = {
    val copied = new WrappedEstimatorModel()
        .setFeaturesCol(getFeaturesCol)
        .setPredictionCol(getpredictionCol)
    copyValues(copied, extra).setParent(parent)
  }


  def transformAndValidate(schema: StructType): StructType = {
    require(schema.map(_.name) contains getFeaturesCol, s"Dataset has to contain the input feature column : $getFeaturesCol")
    schema.add(StructField(getPredictionCol, DoubleType, true))
  }

  def transformSchema(schema: StructType): StructType = transformAndValidate(schema)

  def transform(dataset: Dataset[_]): DataFrame = {
    transformAndValidate(dataset.schema)

    // Make RDD[LabeledPoint]

    // Feed to model
    ???
  }
}

