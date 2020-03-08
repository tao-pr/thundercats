package org.apache.spark.mllib.stat.correlation

import org.apache.spark.rdd.RDD

object ExposedPearsonCorrelation {
  def computeCorrelation(a: RDD[Double], b: RDD[Double]): Double = 
    PearsonCorrelation.computeCorrelation(a,b)
}