package com.tao.thundercats.model

import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Column,Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import org.apache.spark.ml.{Transformer, PipelineModel}
import org.apache.spark.ml.{Pipeline, Estimator, PipelineStage}
import org.apache.spark.ml.param._
import org.apache.spark.ml.feature.{PCA => SparkPCA}
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.ml.Model
import org.apache.spark.mllib.linalg.SingularValueDecomposition

import com.tao.thundercats.physical._
import com.tao.thundercats.functional._
import com.tao.thundercats.physical.Implicits._
import com.tao.thundercats.estimator._
import com.tao.thundercats.evaluation._

object DimReduc {

  private val TEMP_INPUT_COL = "--temp--dim-reduc--"

  sealed trait DimensionReduction {
    def asPipelineStage: PipelineStage
  }

  case class PCA(nComponents: Int) extends DimensionReduction {
    override def asPipelineStage = new Pipeline().setStages(
      Array(
        new SparkPCA()
          .setInputCol("features")
          .setOutputCol("features_reduced")
          .setK(nComponents),
        new ReplaceFeatureColumn()
      )
    )
  }

  case class SVD(nComponents: Int) extends DimensionReduction {
    override def asPipelineStage = new Pipeline().setStages(
      new SVDSelect()
        .setInputCol("features")
        .setOutputCol("features_reduced")
        .setK(nComponents),
      new ReplaceFeatureColumn()
    )
  }
}

class SVDSelect(override val uid: String = Identifiable.randomUID("SVDSelect"))
extends Estimator[SVDSelectModel]
with DefaultParamsWritable
with SVDSelectParams {

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)

  override def transformSchema(schema: StructType) = ???

  override def fit(dataset: Dataset[_]): ReplaceFeatureColumnModel = {
    new SVDSelectModel()
      .setInputCol()
      .setOutputCol()
      .setK(nComponents)
  }

  def setInputCol(value: String): this.type = set(inputCol, value)
  def setOutputCol(value: String): this.type = set(outputCol, value)
}

trait SelectSVDParams extends Params
with HasInputColExposed
with HasOutputColExposed {
  setDefault(inputCol -> "input")
  setDefault(outputCol -> "input2")
  // TAOTODO: Define K as param
}

class SelectSVDModel(
  override val uid: String = Identifiable.randomUID("SelectSVDModel"))
extends Model[SelectSVDModel] 
with SelectSVDParams {

  final def setInputCol(value: String): this.type = set(inputCol, value)
  final def setOutputCol(value: String): this.type = set(outputCol, value)

  override def copy(extra: ParamMap): SelectSVDModel = {
    val copied = new SelectSVDModel()
        .setInputCol(getInputCol)
        .setOutputCol(getOutputCol)
        .setK(getK)
    copyValues(copied, extra).setParent(parent)
  }


  def transformAndValidate(schema: StructType): StructType = {
    require(schema.map(_.name) contains getInputCol, s"Dataset has to contain the input column for SVD : $getInputCol")
    val (originalDataType, originalNullable) = schema.toList.collect {
      case StructField(c, ct, b, _) if c == getInputCol =>
        (ct, b)
    }.head

    schema.add(StructField(getOutputCol, originalDataType, originalNullable))
  }

  def transformSchema(schema: StructType): StructType = transformAndValidate(schema)

  def transform(dataset: Dataset[_]): DataFrame = {
    transformAndValidate(dataset.schema)
    ??? // TAOTODO implement SVD
  }
}


class ReplaceFeatureColumn(override val uid: String = Identifiable.randomUID("ReplaceFeatureColumn"))
extends Estimator[ReplaceFeatureColumnModel]
with DefaultParamsWritable {

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)

  override def transformSchema(schema: StructType) = 
    StructType(schema.filterNot{ p => p.name == "features_reduced" })

  override def fit(dataset: Dataset[_]): ReplaceFeatureColumnModel = {
    new ReplaceFeatureColumnModel()
  }
}

class ReplaceFeatureColumnModel(
  override val uid: String = Identifiable.randomUID("ReplaceFeatureColumnModel"))
extends Model[ReplaceFeatureColumnModel] {

  override def copy(extra: ParamMap): ReplaceFeatureColumnModel = {
    val copied = new ReplaceFeatureColumnModel()
    copyValues(copied, extra).setParent(parent)
  }

  def transformAndValidate(schema: StructType): StructType = {
    require(schema.map(_.name) contains "features_reduced", s"Dataset should have features_reduced column")
    StructType(schema.filterNot{ p => p.name == "features_reduced" })
  }

  def transformSchema(schema: StructType): StructType = transformAndValidate(schema)

  def transform(dataset: Dataset[_]): DataFrame = {
    transformAndValidate(dataset.schema)
    dataset.drop("features").withColumnRenamed("features_reduced", "features")
  }
}
