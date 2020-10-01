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


class DebugStep(override val uid: String = Identifiable.randomUID("DebugStep"))
extends Estimator[DebugStepModel]
with DefaultParamsWritable {

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)
  override def transformSchema(schema: StructType) = schema
  override def fit(dataset: Dataset[_]): DebugStepModel = new DebugStepModel()
}

class DebugStepModel(override val uid: String = Identifiable.randomUID("DebugStepModel"))
extends Model[DebugStepModel] {

  override def copy(extra: ParamMap): DebugStepModel = new DebugStepModel()
  def transformAndValidate(schema: StructType): StructType = schema
  def transformSchema(schema: StructType): StructType = transformAndValidate(schema)
  def transform(dataset: Dataset[_]): DataFrame = {
    transformAndValidate(dataset.schema)

    Console.println("Debugging dataframe")
    dataset.printSchema
    dataset.toDF
  }
}





