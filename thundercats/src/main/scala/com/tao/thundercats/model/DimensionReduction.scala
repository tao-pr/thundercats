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

  sealed trait DimensionReduction {
    def asPipelineStage: PipelineStage
  }

  // TAOTODO: Need a transformer which renames "features" -> "temp"
  // before feeding into DimReduc

  case class PCA(nComponents: Int) extends DimensionReduction
  case class LDA(nComponents: Int) extends DimensionReduction

}
