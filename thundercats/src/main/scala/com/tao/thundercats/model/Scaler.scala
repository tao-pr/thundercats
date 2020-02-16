package com.tao.thundercats.model

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

import java.io.File
import sys.process._
import scala.reflect.io.Directory
import scala.util.Try

import com.tao.thundercats.physical._
import com.tao.thundercats.functional._
import com.tao.thundercats.physical.Implicits._

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

  override def setInputCol(value: String): this.type = set(inputCol, value)
  override def setOutputCol(value: String): this.type = set(outputCol, value)

  setDefault(logScale -> false)
  setDefault(norm -> true)

  override def fit(dataset: Dataset[_]): ScalerModel = {
    transformSchema(dataset.schema, logging=true)
    if ($(norm)) {
      val values = dataset.rdd.map(row => row.getAs[Double]($(inputCol))).collect
      new ScalerModel(values.sum, values.min, $(logScale))
    } else new ScalerModel(0, 0, $(logScale))
  }
}

trait ScalerParams extends Params
with InputColExposed
with OutputColExposed {
  final val norm = new Param[String](this, "inputCol", "The input column")
  final val outputCol = new Param[String](this, "outputCol", "The output column")
  final def logScale: Param[Boolean] = new Param[Boolean](this, "logScale", "Boolean indicating whether log scale is used.")
  final def norm: Param[Boolean] = new Param[Boolean](this, "norm", "Turning on or off normalisation scaler")

  final def getLogScale: Boolean = $(logScale)
  final def setLogScale(value: Boolean) = set(logScale, value)

  final def getNorm: Boolean = $(norm)
  final def setNorm(value: Boolean) = set(norm, value)
}

class ScalerModel(
  sum: Double,
  min: Double,
  logScale: Boolean,
  override val uid: String = Identifiable.randomUID("ScalerModel"))
extends Model[ScalerModel] 
with ScalerParams {

  override def copy(extra: ParamMap): ArrayEncoderModel[T] = {
    val copied = new ArrayEncoderModel[T](uid, labels)
    copyValues(copied, extra).setParent(parent)
  }

  def transformSchema(schema: StructType): StructType = transformAndValidate(schema)

  private def scale(dataset: Dataset[_]): DataFrame = {
    dataset.withColumn($(outputCol), (col($(inputCol)) - min) / sum)
  }

  def transform(dataset: Dataset[_]): DataFrame = {
    transformAndValidate(dataset.schema)
    val scaledDf = if (sum>0) scale(dataset) else dataset.withColumn($(outputCol), col($(inputCol)))
    if (logScale) 
      scaledDf.withColumn($(outputCol), logNatural($(outputCol)))
    else 
      scaledDf
  }
}