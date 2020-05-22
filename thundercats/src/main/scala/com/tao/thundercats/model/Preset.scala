package com.tao.thundercats.model

import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.feature.VectorAssembler

import com.tao.thundercats.evaluation._

/**
 * Preset of simple estimators
 */
object Preset {
  def linearReg(
    features: FeatureColumn, 
    labelCol: String, 
    outputCol: String,
    maxIters: Int = 10) =  
      new Pipeline().setStages(Array(new LinearRegression()
        .setFeaturesCol(features.colName)
        .setPredictionCol(outputCol)
        .setLabelCol(labelCol)
        .setMaxIter(maxIters)))
}