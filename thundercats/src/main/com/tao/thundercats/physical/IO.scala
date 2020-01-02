package com.tao.thundercats.physical

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Column,Row}
import org.apache.spark.sql.catalyst.encoders._
import org.apache.spark.sql.{Encoders, Encoder}
import org.apache.spark.sql.avro._
import org.apache.spark.sql.functions._

import com.tao.thundercats.monad.Generic._

object IO {
  def readParquet(path: String): Dataset[Row]
  def readParquetAs[A](path: String)(implicit encoder: Encoder[A]): Dataset[A]
  def readCSV(path: String, header: Boolean=true): Dataset[Row]
  def readCSVAs[A](path: String, header: Boolean=true)(implicit encoder: Encoder[A]): Dataset[A]
  def readKafka(topic: String, limit: Option[Int]=None): Dataset[Row]
  def readKafkaAs[A](topic: String, limit: Option[Int]=None)(implicit encoder: Encoder[A]): Dataset[A]

  def writeParquet(df: Dataset[A],path: String): Dataset[A]
}
