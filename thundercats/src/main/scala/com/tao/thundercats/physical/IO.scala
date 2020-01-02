package com.tao.thundercats.physical

import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Column,Row}
import org.apache.spark.sql.catalyst.encoders._
import org.apache.spark.sql.{Encoders, Encoder}
import org.apache.spark.sql.avro._
import org.apache.spark.sql.functions._

// Data Monad
trait M[A] {
  def unit(a: A): M[A]
  def flatMap(f: A => M[A]): M[A]
}

// IO read/write monad
private [physical] trait Data extends M[DataFrame]{
  protected def read: DataFrame
  override def unit(a: DataFrame): M[DataFrame]
  override def flatMap(f: DataFrame => M[DataFrame]): M[DataFrame] = {
    unit(read)
  }
}

case class DataWrap(df: DataFrame) extends Data[DataFrame] {
  override protected def read: DataFrame = df
  override def unit(a: DataFrame): M[DataFrame] = DataWrap(a)
}

object Read {
  case class CSV[A](path: String, withHeader: Boolean = true)
  (implicit val spark: SparkSession) extends Data {
    override protected def read: DataFrame = {
      import spark.implicits._
      spark
        .read
        .option("header", withHeader.toString)
        .option("inferSchema", "true")
        .csv(path)
    }
  }
  case class Parquet[A](path: String) extends Data {
    override protected def read: DataFrame = {
      import spark.implicits._
      spark
        .read
        .parquet(path)
    }
  }
}

object Write {
  
  trait Partition
  case object NoPartition extends Partition
  case class PartitionCol(cols: List[String]) extends Partition

  private trait Generic {
    val df: DataFrame
    val partition: Partition
    private def preprocess = partition match {
      case NoPartition => df.coalesce(1).write
      case PartitionCol(cols) => df.write.partitionBy(cols:_*)
    }
  }

  case class CSV[A](override val df: DataFrame, override val partition: Partition = NoPartition)
  (implicit val spark: SparkSession) extends Generic with Data {
    override protected def read: DataFrame = {
      import spark.implicits._
      preprocess
        .option("header", withHeader.toString)
        .option("inferSchema", "true")
        .csv(path)
      df
    }
  }
  case class Parquet[A](override val df: DataFrame, override val partition: Partition = NoPartition)
  (implicit val spark: SparkSession) extends Generic with Data {
    override protected def read: DataFrame = {
      import spark.implicits._
      preprocess.parquet(path)
      df
    }
  }
}
