package com.tao.thundercats.preprocess

import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Column,Row}
import org.apache.spark.sql.catalyst.encoders._
import org.apache.spark.sql.{Encoders, Encoder}
import org.apache.spark.rdd.DoubleRDDFunctions
import org.apache.spark.sql.avro._
import org.apache.spark.sql.{functions => F}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.rdd.RDD

import com.tao.thundercats.physical._
import com.tao.thundercats.functional._

object Text {

  def trim(df: DataFrame, c: String): MayFail[DataFrame] = MayFail {
    df.withColumn(c, F.trim(col(c)))
  }
}