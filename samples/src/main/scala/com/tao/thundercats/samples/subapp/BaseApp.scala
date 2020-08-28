package com.tao.thundercats.samples.subapp

import com.tao.thundercats.samples.base._

import org.apache.spark.sql.SparkSession

trait BaseApp {
  def runMe(implicit spark: SparkSession): Unit
}