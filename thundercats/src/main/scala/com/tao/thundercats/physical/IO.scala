package com.tao.thundercats.physical

import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Column,Row}
import org.apache.spark.sql.catalyst.encoders._
import org.apache.spark.sql.{Encoders, Encoder}
import org.apache.spark.sql.avro._
import org.apache.spark.sql.functions._

import scala.util.Try

object Screen {
  def log(df: DataFrame, title: Option[String]=None): Option[DataFrame] = {
    title.map(t => Console.println(Console.CYAN + title + Console.RESET))
    Console.println(Console.CYAN)
    df.show(5, false)
    Console.println(Console.RESET)
    Some(df)
  }

  def logStream(df: DataFrame, title: Option[String]=None): Option[DataFrame] = {
    title.map(t => Console.println(Console.CYAN + title + Console.RESET))
    Console.println(Console.CYAN)
    val q = df.writeStream
      .outputMode("append")
      .format("console")
      .start()
    q.awaitTermination(50)
    Console.println(Console.RESET)
    Some(df)
  }
}

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

  def kafkaStream(topic: String, serverAddr: String, port: Int = 9092)
  (implicit spark: SparkSession): Option[DataFrame] = {
    Try {
      val df = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", s"${serverAddr}:${port}")
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      Some(df)
    } getOrElse(None)
  }

  def kafka(topic: String, serverAddr: String, port: Int = 9092)
  (implicit spark: SparkSession): Option[DataFrame] = {
    Try {
      val df = spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", s"${serverAddr}:${port}")
        .option("subscribe", topic)
        .load()
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
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

  def kafkaStream(
    df: DataFrame, 
    topic: String, 
    serverAddr: String, 
    port: Int = 9092,
    checkpointLocation: String = "./chk"): Option[DataFrame] = {
    val q = df.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", s"${serverAddr}:${port}")
      .option("topic", topic)
      .option("outputMode", "append")
      .option("checkpointLocation", checkpointLocation)
      .start()
    q.awaitTermination()
    Some(df)
  }

  def kafka(df: DataFrame, topic: String, serverAddr: String, port: Int = 9092): Option[DataFrame] = {
    df.write
      .format("kafka")
      .option("kafka.bootstrap.servers", s"${serverAddr}:${port}")
      .option("topic", topic)
      .save()
    Some(df)
  }
}
