package com.tao.thundercats.model

import org.apache.spark.ml.feature._
import org.apache.spark.ml.{Estimator, Model, Transformer}
import org.apache.spark.ml.attribute.{Attribute, NominalAttribute}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.{HasHandleInvalid, HasInputCol, HasOutputCol}
import org.apache.spark.ml.util._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions.{log => loge, _}
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
extends Normalizer {

  final def logScale: Param[Boolean] = new Param[Boolean](this, "logScale", "Boolean indicating whether log scale is used.")

  setDefault(logScale -> false)

  final def getLogScale: Boolean = $(logScale)
  final def setLogScale(value: Boolean) = set(logScale, value)

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)

  override def transformSchema(schema: StructType) = 
    schema.add($(outputCol), ArrayType(DoubleType, true), true)

  // def setInputCol(value: String): this.type = set(inputCol, value)
  // def setOutputCol(value: String): this.type = set(outputCol, value)

  override def transform(dfRaw: Dataset[_]): Dataset[Row] = {
    transformSchema(df.schema, logging=true)
    // Disable normalisation by set [[p]] to zero or negative
    val df = if ($(p)<=0) dfRaw.withColumn($(outputCol), col($(inputCol))) else super.transform(dfRaw)
    if ($(logScale)) 
      df.withColumn($(outputCol), loge(col($(outputCol))))
    else 
      df
  }
}