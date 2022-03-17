package com.tao.thundercats.evaluation

import com.tao.thundercats.functional._
import org.apache.spark.sql.DataFrame

private [evaluation] trait BaseMeasure[A] {
  def % (df: DataFrame, specimen: Specimen): MayFail[A]
  def isBetter(a: A, b: A): Boolean
  def className: String = getClass.getName.split('.').last.replace("$","")
}
