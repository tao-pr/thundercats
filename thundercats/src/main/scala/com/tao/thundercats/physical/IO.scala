package com.tao.thundercats.physical

import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Column,Row}
import org.apache.spark.sql.catalyst.encoders._
import org.apache.spark.sql.{Encoders, Encoder}
import org.apache.spark.sql.avro._
import org.apache.spark.sql.functions._

import scala.util.Try

object Read {
  def csv(path: String, withHeader: Boolean = true)
  (implicit spark: SparkSession): Option[DataFrame] = {
    import spark.implicits._
    Try {
      val df = spark
        .read
        .option("header", withHeader.toString)
        .option("inferSchema", "true")
        .csv(path)  
      Some(df)
    } getOrElse(None)
  }
  
  def parquet(path: String) 
  (implicit spark: SparkSession): Option[DataFrame] = {
    import spark.implicits._
    Try {
      val df = spark
        .read
        .parquet(path)
      Some(df)
    } getOrElse(None)
  }
}

object Write {
  
  trait Partition
  case object NoPartition extends Partition
  case class PartitionCol(cols: List[String]) extends Partition

  protected def preprocess(df: DataFrame, partition: Partition) = partition match {
    case NoPartition => df.coalesce(1).write
    case PartitionCol(cols) => df.write.partitionBy(cols:_*)
  }

  def csv(
    df: DataFrame, 
    path: String,
    partition: Partition = NoPartition)
  (implicit spark: SparkSession): Option[DataFrame] = {
    import spark.implicits._
    preprocess(df, partition)
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)
    Some(df)
  }

  def parquet(
    df: DataFrame, 
    path: String,
    partition: Partition = NoPartition)
  (implicit spark: SparkSession): Option[DataFrame] = {
    import spark.implicits._
    preprocess(df, partition).parquet(path)
    Some(df)
  }
}
