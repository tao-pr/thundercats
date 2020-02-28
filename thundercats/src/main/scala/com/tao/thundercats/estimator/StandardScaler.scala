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

class StandardScaler(override val uid: String = Identifiable.randomUID("StandardScaler"))
extends Estimator[StandardScalerModel]
with StandardScalerParams
with DefaultParamsWritable {

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)

  override def transformSchema(schema: StructType) = 
    schema.add($(outputCol), DoubleType, true)

  def setInputCol(value: String): this.type = set(inputCol, value)
  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def fit(dataset: Dataset[_]): ScalerModel = {
    ???
  }
}

trait StandardScalerParams extends Params
with HasInputColExposed
with HasOutputColExposed {
  setDefault(inputCol -> "input")
  setDefault(outputCol -> "output")

  def getInputCol = $(inputCol)
  def getOutputCol = $(outputCol)
}

class StandardScalerModel(
  mean: Double,
  std: Double,
  override val uid: String = Identifiable.randomUID("StandardScalerModel"))
extends Model[StandardScalerModel] 
with StandardScalerParams {
  
  override def copy(extra: ParamMap): ScalerModel = {
    val copied = new StandardStandardScalerModel(mean, std)
        .setInputCol(getInputCol)
        .setOutputCol(getOutputCol)
    copyValues(copied, extra).setParent(parent)
  }

  def transformAndValidate(schema: StructType): StructType = {
    require(schema.map(_.name) contains getInputCol, s"Dataset has to contain the input column : $getInputCol")
    schema.add(StructField(getOutputCol, DoubleType, false))
  }

  def transformSchema(schema: StructType): StructType = transformAndValidate(schema)

  def transform(dataset: Dataset[_]): DataFrame = {
    transformAndValidate(dataset.schema)

    ???

  }
}