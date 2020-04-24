package com.tao.thundercats.estimator

import org.apache.spark.ml.feature._
import org.apache.spark.ml.{Estimator, Model, Transformer}
import org.apache.spark.ml.attribute.{Attribute, NominalAttribute}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.{HasHandleInvalid, HasInputCol, HasOutputCol}
import org.apache.spark.ml.util._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions.{log => logNatural, _}
import org.apache.spark.sql.types._
import org.apache.spark.ml.util.MLWriter
import org.apache.spark.SparkException
import scala.reflect.runtime.universe._
import scala.reflect.ClassTag
import org.apache.hadoop.fs.Path

import org.apache.spark.ml.param.shared._

import java.io.File
import sys.process._
import scala.reflect.io.Directory
import scala.util.Try

import com.tao.thundercats.physical._
import com.tao.thundercats.functional._
import com.tao.thundercats.physical.Implicits._
import com.tao.thundercats.model._

/**
 * Scale numerical columns 
 */
class Scaler(override val uid: String = Identifiable.randomUID("Scaler"))
extends Estimator[ScalerModel]
with ScalerParams
with DefaultParamsWritable {

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)

  override def transformSchema(schema: StructType) = 
    schema.add($(outputCol), DoubleType, true)

  def setInputCol(value: String): this.type = set(inputCol, value)
  def setOutputCol(value: String): this.type = set(outputCol, value)

  final def getLogScale: Boolean = $(logScale)
  final def setLogScale(value: Boolean) = set(logScale, value)

  final def getNorm: Boolean = $(norm)
  final def setNorm(value: Boolean) = set(norm, value)

  override def fit(dataset: Dataset[_]): ScalerModel = {
    transformSchema(dataset.schema, logging=true)
    if ($(norm)) {
      val values = dataset.toDF.getDoubleRDD($(inputCol)).collect
      new ScalerModel(values.sum, values.min)
        .setLogScale($(logScale))
        .setNorm($(norm))
        .setInputCol($(inputCol))
        .setOutputCol($(outputCol))
    } else new ScalerModel(0, 0)
        .setLogScale($(logScale))
        .setNorm($(norm))
        .setInputCol($(inputCol))
        .setOutputCol($(outputCol))
  }
}

trait ScalerParams extends Params
with HasInputColExposed
with HasOutputColExposed {
  final def logScale: Param[Boolean] = new Param[Boolean](this, "logScale", "Boolean indicating whether log scale is used.")
  final def norm: Param[Boolean] = new Param[Boolean](this, "norm", "Turning on or off normalisation scaler")  

  setDefault(inputCol -> "input")
  setDefault(outputCol -> "output")
  setDefault(logScale -> false)
  setDefault(norm -> true)
}

class ScalerModel(
  sum: Double,
  min: Double,
  override val uid: String = Identifiable.randomUID("ScalerModel"))
extends Model[ScalerModel] 
with ScalerParams {

  final def getLogScale: Boolean = $(logScale)
  final def setLogScale(value: Boolean) = set(logScale, value)

  final def getNorm: Boolean = $(norm)
  final def setNorm(value: Boolean) = set(norm, value)

  final def setInputCol(value: String): this.type = set(inputCol, value)
  final def setOutputCol(value: String): this.type = set(outputCol, value)

  override def copy(extra: ParamMap): ScalerModel = {
    val copied = new ScalerModel(sum, min)
        .setLogScale($(logScale))
        .setNorm($(norm))
        .setInputCol($(inputCol))
        .setOutputCol($(outputCol))
    copyValues(copied, extra).setParent(parent)
  }

  def transformAndValidate(schema: StructType): StructType = {
    val inputColumn = $(inputCol)
    val outputColumn = $(outputCol)
    require(schema.map(_.name) contains inputColumn, s"Dataset has to contain the input column : $inputColumn")
    schema.add(StructField(outputColumn, DoubleType, false))
  }

  def transformSchema(schema: StructType): StructType = transformAndValidate(schema)

  private def scale(dataset: Dataset[_]): DataFrame = {
    dataset.withColumn($(outputCol), col($(inputCol)) / sum)
  }

  def transform(dataset: Dataset[_]): DataFrame = {
    transformAndValidate(dataset.schema)
    val scaledDf = if (sum>0) scale(dataset) else dataset.withColumn($(outputCol), col($(inputCol)))
    if ($(logScale))
      scaledDf.withColumn($(outputCol), logNatural($(outputCol)))
    else 
      scaledDf
  }
}