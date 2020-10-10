package com.tao.thundercats.estimator

import org.apache.spark.ml.feature._
import org.apache.spark.ml.{Estimator, Model, Transformer}
import org.apache.spark.ml.attribute.{Attribute, NominalAttribute}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.{HasHandleInvalid, HasInputCol, HasOutputCol}
import org.apache.spark.ml.util._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.types._
import org.apache.spark.SparkException

import org.apache.spark.ml.param.shared._
import org.apache.spark.mllib.regression._

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

  override def transformSchema(schema: StructType) = 
    schema.add($(outputCol), DoubleType, true)

  def setFeaturesCol(value: String): this.type = set(featuresCol, value)
  def setOutputCol(value: String): this.type = set(outputCol, value)
  def setLabelCol(value: String): this.type = set(labelCol, value)

  override def fit(dataset: Dataset[_]): ColumnRenameModel = {
}

trait WrappedEstimatorParams extends Params
with HasFeaturesColExposed
with HasLabelColExposed
with HasOutputColExposed {
  setDefault(inputCol -> "input")
  setDefault(outputCol -> "input2")
}