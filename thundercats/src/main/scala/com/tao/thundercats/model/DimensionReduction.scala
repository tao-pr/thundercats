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

  private val TEMP_INPUT_COL = "--temp--"

  sealed trait DimensionReduction {
    def asPipelineStage: PipelineStage
  }

  case class PCA(nComponents: Int) extends DimensionReduction {
    override def asPipelineStage = new Pipeline().setStages(
      Array(
        new ColumnRename()
          .setInputCol("features1") // TAODEBUG expecting failure
          .setOutputCol(TEMP_INPUT_COL),
        new SparkPCA()
          .setInputCol(TEMP_INPUT_COL)
          .setOutputCol("features")
          .setK(nComponents)
        ))
  }

  // TAOTODO add other dim reductions

}
