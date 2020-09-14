package com.tao.thundercats.model

import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.feature.VectorAssembler

import com.tao.thundercats.evaluation._

/**
 * Preset of simple estimators
 */
object Preset {

  // Simple linear regression
  // elasticNetParam = 0 -> ridge
  // elasticNetParam = 1 -> lasso
  def linearReg(
    features: FeatureColumn, 
    labelCol: String, 
    outputCol: String,
    maxIters: Int = 10,
    elasticNetParam: Option[Double] = None) =  {
      val lg = new LinearRegression()
        .setFeaturesCol(features.colName)
        .setPredictionCol(outputCol)
        .setLabelCol(labelCol)
        .setMaxIter(maxIters)
      new Pipeline().setStages(Array(elasticNetParam match {
        case None => lg
        case Some(p) => lg.setElasticNetParam(p)
      }))
    }

  def decisionTree(
    features: FeatureColumn,
    labelCol: String,
    outputCol: String,
    impurity: String="entropy",
    maxDepth: Int = 5) = {
    val tree = new DecisionTreeClassifier()
      .setFeaturesCol(features.colName)
      .setLabelCol(labelCol)
      .setMaxDepth(maxDepth)
      .setImpurity(impurity)
      .setPredictionCol(outputCol)
      .setRawPredictionCol(s"${outputCol}_raw")
    new Pipeline().setStages(Array(tree))
  }

  def randomForest(
    features: FeatureColumn,
    labelCol: String,
    outputCol: String,
    maxIters: Int = 10,
    maxDepth: Int = 5,
    elasticNetParam: Option[Double] = None) = {

    ???
  }
}