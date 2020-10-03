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

import java.io.File
import sys.process._
import scala.reflect.io.Directory
import scala.util.Try

import com.tao.thundercats.physical._


class ColumnRename(override val uid: String = Identifiable.randomUID("ColumnRename"))
extends Estimator[ColumnRenameModel]
with ColumnRenameParams
with DefaultParamsWritable {

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)

  override def transformSchema(schema: StructType) = 
    schema.add($(outputCol), DoubleType, true)

  def setInputCol(value: String): this.type = set(inputCol, value)
  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def fit(dataset: Dataset[_]): ColumnRenameModel = {
    new ColumnRenameModel()
      .setInputCol($(inputCol))
      .setOutputCol($(outputCol))
  }
}

trait ColumnRenameParams extends Params
with HasInputColExposed
with HasOutputColExposed {
  setDefault(inputCol -> "input")
  setDefault(outputCol -> "input2")
}

class ColumnRenameModel(
  override val uid: String = Identifiable.randomUID("ColumnRenameModel"))
extends Model[ColumnRenameModel] 
with ColumnRenameParams {

  final def setInputCol(value: String): this.type = set(inputCol, value)
  final def setOutputCol(value: String): this.type = set(outputCol, value)

  override def copy(extra: ParamMap): ColumnRenameModel = {
    val copied = new ColumnRenameModel()
        .setInputCol(getInputCol)
        .setOutputCol(getOutputCol)
    copyValues(copied, extra).setParent(parent)
  }


  def transformAndValidate(schema: StructType): StructType = {
    require(schema.map(_.name) contains getInputCol, s"Dataset has to contain the input feature column : $getInputCol")
    val (originalDataType, originalNullable) = schema.toList.collect {
      case StructField(c, ct, b, _) if c == getInputCol =>
        (ct, b)
    }.head

    schema.add(StructField(getOutputCol, originalDataType, originalNullable))
  }

  def transformSchema(schema: StructType): StructType = transformAndValidate(schema)

  def transform(dataset: Dataset[_]): DataFrame = {
    transformAndValidate(dataset.schema)
    dataset.withColumnRenamed(getInputCol, getOutputCol).cache
  }
}