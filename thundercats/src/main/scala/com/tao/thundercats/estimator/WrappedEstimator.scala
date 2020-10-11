package com.tao.thundercats.estimator

import org.apache.spark.ml.feature._
import org.apache.spark.ml.{Estimator, Model, Transformer}
import org.apache.spark.ml.attribute.{Attribute, NominalAttribute}
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.ml.param.shared._
import org.apache.spark.mllib.regression._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.types._
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.SparkException
import org.apache.spark.mllib.regression.LabeledPoint

import java.io.File
import sys.process._
import scala.reflect.io.Directory
import scala.util.Try

import com.tao.thundercats.physical._

/**
 * Wrap a [[GeneralizedLinearAlgorithm]] into a [[Transformer]]
 */
case class WrappedEstimator(
  estimator: GeneralizedLinearAlgorithm[GeneralizedLinearModel],
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
    val trainData = dataset.toDF.rdd.map{ row => LabeledPoint(
      row.getAs[Double](getLabelCol),
      row.getAs[Vector](getFeaturesCol)
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
with HasPredictionColExposed {
  setDefault(featuresCol -> "features")
  setDefault(predictionCol -> "prediction")
  setDefault(labelCol -> "label")
}

class WrappedEstimatorModel(
  model: GeneralizedLinearModel,
  override val uid: String = Identifiable.randomUID("linalg"))
extends Model[WrappedEstimatorModel] 
with WrappedEstimatorParams {

  final def setFeaturesCol(value: String): this.type = set(featuresCol, value)
  final def setPredictionCol(value: String): this.type = set(predictionCol, value)

  override def copy(extra: ParamMap): WrappedEstimatorModel = {
    val copied = new WrappedEstimatorModel(model)
        .setFeaturesCol(getFeaturesCol)
        .setPredictionCol(getPredictionCol)
    copyValues(copied, extra).setParent(parent)
  }

  def transformAndValidate(schema: StructType): StructType = {
    require(schema.map(_.name) contains getFeaturesCol, s"Dataset has to contain the input feature column : $getFeaturesCol")
    schema.add(StructField(getPredictionCol, DoubleType, true))
  }

  def transformSchema(schema: StructType): StructType = transformAndValidate(schema)

  def transform(dataset: Dataset[_]): DataFrame = {
    transformAndValidate(dataset.schema)

    import dataset.sparkSession.implicits._

    // Make RDD[Vector]
    val predData = dataset.toDF.rdd.map{ row =>
      val pred = model.predict(row.getAs[Vector](getFeaturesCol))
      val old = row.toSeq.toList
      Row.fromSeq(old :+ pred)
    }

    predData.toDF
  }
}

