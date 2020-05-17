package com.tao.thundercats.model

import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.classification.LogisticRegression
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
    maxIters: Int = 10) = {
    val (featureCol, initPipes): (String, Array[PipelineStage]) = features match {
      case Feature(c) => (c, Array.empty)
      case AssemblyFeature(arr,featCol) => 
        ("features", Array(
          new VectorAssembler()
            .setInputCols(arr.toArray)
            .setOutputCol(featCol)))
    }
    new Pipeline().setStages(
      initPipes ++ Array(
      new LogisticRegression()
        .setFeaturesCol(featureCol)
        .setPredictionCol(outputCol)
        .setLabelCol(labelCol)
        .setMaxIter(maxIters))
    )
  }
}