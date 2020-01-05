package com.tao.thundercats.physical

import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Column,Row}
import org.apache.spark.sql.catalyst.encoders._
import org.apache.spark.sql.{Encoders, Encoder}
import org.apache.spark.sql.avro._
import org.apache.spark.sql.functions._

import scala.util.Try

object ColumnEncoder {
  private [physical] trait Encoder 
  case object None extends Encoder
  case class Avro(schema: String) extends Encoder
}

object Screen {
  def showDF(df: DataFrame, title: Option[String]=None): Option[DataFrame] = {
    title.map(t => Console.println(Console.CYAN + title + Console.RESET))
    Console.println(Console.CYAN)
    df.show(5, false)
    Console.println(Console.RESET)
    Some(df)
  }

  def showDFStream(df: DataFrame, title: Option[String]=None): Option[DataFrame] = {
    title.map(t => Console.println(Console.CYAN + t + Console.RESET))
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
  def csv(path: String, withHeader: Boolean = true, delimiter: String = ",")
  (implicit spark: SparkSession): Option[DataFrame] = {
    import spark.implicits._
    Try {
      val df = spark
        .read
        .option("header", withHeader.toString)
        .option("inferSchema", "true")
        .option("delimiter", delimiter)
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

  def kafkaStream(
    topic: String, 
    serverAddr: String, 
    port: Int = 9092, 
    offset: Option[Int] = None,
    colEncoder: ColumnEncoder.Encoder = ColumnEncoder.None)
  (implicit spark: SparkSession): Option[DataFrame] = {
    import spark.implicits._
    Try {
      val df = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", s"${serverAddr}:${port}")
        .option("subscribe", topic)
        .option("startingOffsets", offset.map(_.toString).getOrElse("earliest"))
        .load()
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

      colEncoder match {
        case ColumnEncoder.None => Some(df)
        case ColumnEncoder.Avro(schema) => Some(df.select(
          from_avro('key, schema).as("key"),
          from_avro('value, schema).as("value")
        ))
      }
    } getOrElse(None)
  }

  def kafka(topic: String, serverAddr: String, port: Int = 9092, colEncoder: ColumnEncoder.Encoder = ColumnEncoder.None)
  (implicit spark: SparkSession): Option[DataFrame] = {
    import spark.implicits._
    Try {
      val df = spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", s"${serverAddr}:${port}")
        .option("subscribe", topic)
        .load()
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      
      colEncoder match {
        case ColumnEncoder.None => Some(df)
        case ColumnEncoder.Avro(schema) => Some(df.select(
          from_avro('key, schema).as("key"),
          from_avro('value, schema).as("value")
        ))
      }
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
    partition: Partition = NoPartition,
    delimiter: String = ",")
  (implicit spark: SparkSession): Option[DataFrame] = {
    import spark.implicits._
    preprocess(df, partition)
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", delimiter)
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
    colEncoder: ColumnEncoder.Encoder = ColumnEncoder.None,
    checkpointLocation: String = "./chk",
    timeout: Option[Int] = None): Option[DataFrame] = {

    import df.sqlContext.implicits._
    val dfEncoded = colEncoder match {
      case ColumnEncoder.None => df
      case ColumnEncoder.Avro(_) => df.select(
        to_avro('key).as("key"),
        to_avro('value).as("value")
      )
    }

    val q = dfEncoded.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", s"${serverAddr}:${port}")
      .option("topic", topic)
      .option("outputMode", "append")
      .option("checkpointLocation", checkpointLocation)
      .start()

    timeout match {
      case None => q.awaitTermination()
      case Some(t) => q.awaitTermination(t)
    }
    
    Some(df)
  }

  def kafka(
    df: DataFrame, 
    topic: String, 
    serverAddr: String, 
    port: Int = 9092,
    colEncoder: ColumnEncoder.Encoder = ColumnEncoder.None): Option[DataFrame] = {
    import df.sqlContext.implicits._
    val dfEncoded = colEncoder match {
      case ColumnEncoder.None => df
      case ColumnEncoder.Avro(_) => df.select(
        to_avro('key).as("key"),
        to_avro('value).as("value")
      )
    }

    dfEncoded.write
      .format("kafka")
      .option("kafka.bootstrap.servers", s"${serverAddr}:${port}")
      .option("topic", topic)
      .save()
    Some(df)
  }

  def streamToFile(
    df: DataFrame,
    fileType: String,
    path: String,
    partition: Partition = NoPartition,
    checkpointLocation: String = "./chk",
    timeout: Option[Int] = None): Option[DataFrame] = {
    assert(Set("parquet", "csv", "orc", "json") contains(fileType))

    val stream = df.writeStream
      .format(fileType)
      .outputMode("append")
      .option("path", path)
      .option("checkpointLocation", checkpointLocation)

    val q = (partition match {
      case NoPartition => stream
      case PartitionCol(cols) => {
        if (cols.size > 1) 
          Console.println(Console.YELLOW + 
            s"Streaming to ${fileType} will only partitioning with ${cols.head}" +
            Console.RESET)
        stream.partitionBy(cols.head)
      }
    }).start()

    timeout match {
      case None => q.awaitTermination()
      case Some(t) => q.awaitTermination(t)
    }
    Some(df)
  }
}
