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
