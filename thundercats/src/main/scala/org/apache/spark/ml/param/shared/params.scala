package org.apache.spark.ml.param.shared

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.param._

trait HasInputColsExposed extends HasInputCols

trait HasInputColExposed extends HasInputCol

trait HasOutputColExposed extends HasOutputCol 

trait HasInOutColExposed extends HasInputCol with HasOutputCol

trait HasFeaturesColExposed extends HasFeaturesCol

trait HasLabelColExposed extends HasLabelCol

trait HasPredictionColExposed extends HasPredictionCol

