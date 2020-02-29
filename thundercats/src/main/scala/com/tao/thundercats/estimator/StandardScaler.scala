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

  override def fit(dataset: Dataset[_]): StandardScalerModel = {
    // Calculate mean and standard deviation
    val df = dataset.toDF
    val sum = df.rdd.map(_.getAs[Double](getInputCol)).reduce(_ + _)
    val mean = sum / df.count.toDouble
    val std = java.lang.Math.sqrt(
      df.rdd
        .map(_.getAs[Double](getInputCol))
        .map(d => (d-mean)*(d-mean))
        .reduce(_ + _) / df.count.toDouble
      )
    new StandardScalerModel(mean, std)
      .setInputCol(getInputCol)
      .setOutputCol(getOutputCol)
  }
}

trait StandardScalerParams extends Params
with HasInputColExposed
with HasOutputColExposed {
  setDefault(inputCol -> "input")
  setDefault(outputCol -> "output")
}

class StandardScalerModel(
  mean: Double,
  std: Double,
  override val uid: String = Identifiable.randomUID("StandardScalerModel"))
extends Model[StandardScalerModel] 
with StandardScalerParams {

  final def setInputCol(value: String): this.type = set(inputCol, value)
  final def setOutputCol(value: String): this.type = set(outputCol, value)
  
  override def copy(extra: ParamMap): StandardScalerModel = {
    val copied = new StandardScalerModel(mean, std)
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

    // Translate and scale
    val standard = udf((x: java.lang.Double) => x match {
      case null => None
      case _    => Some((x - mean)/std)
    })
    dataset.toDF.withColumn(getOutputCol, standard(col(getInputCol)))
  }
}