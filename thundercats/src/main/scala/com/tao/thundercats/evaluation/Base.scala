package com.tao.thundercats.evaluation

import org.apache.spark.sql.{Dataset, DataFrame}

import com.tao.thundercats.physical._
import com.tao.thundercats.functional._
import com.tao.thundercats.physical.Implicits._
import com.tao.thundercats.estimator._

private [evaluation] trait BaseMeasure[A] {
  def % (df: DataFrame, specimen: Specimen): MayFail[A]
  def isBetter(a: A, b: A): Boolean
  def className: String = getClass.getName.split('.').last.replace("$","")
}