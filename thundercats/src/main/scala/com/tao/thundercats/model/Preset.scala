package com.tao.thundercats.model

import org.apache.spark.ml.Pipeline
import org.apache.spark.mllib.classification.LogisticRegression
import import org.apache.spark.ml.feature.VectorAssembler

import com.tao.thundercats.evaluation._

/**
 * Preset of simple estimators
 */
object Preset {
  def linearReg(features: FeatureColumn, labelCol: String, outputCol: String,
    maxIters: Int = 10) = {
    new Pipeline().setStages(
      new LogisticRegression()
        .setFeaturesCol(features.asArray)
        .setPredictionCol(outputCol)
        .setLabelCol(labelCol)
        .setMaxIter(maxIters)
    )
  }
}