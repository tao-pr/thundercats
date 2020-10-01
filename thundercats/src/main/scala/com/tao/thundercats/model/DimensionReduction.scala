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
        new DebugStep()
      )
    )
  }
}
