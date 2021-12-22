package com.tao.thundercats.physical

import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Column,Row}
import org.apache.spark.sql.catalyst.encoders._
import org.apache.spark.sql.{Encoders, Encoder}
import org.apache.spark.sql.avro._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.util.Try

import com.tao.thundercats.functional._

object ColumnEncoder {
  private [physical] trait Encoder
  case object None extends Encoder
  case class Avro(schema: String) extends Encoder
}

object Show {
  private [physical] trait Opt
  case object Default extends Opt // Spark default, no col trunc
  case object Truncate extends Opt
  case object HideComplex extends Opt
  case class Max(numRows: Int) extends Opt 
}

object Screen {
  /**
   * Disguise complex columns of a dataframe
   */
  private def simplify(df: DataFrame): DataFrame = {
    df.schema.toList.foldLeft(df){ case(df,c) => c match {
        case StructField(colName, ArrayType(StringType,_), _, _) =>
          df.withColumn(colName, lit("<array<str>>"))
        case StructField(colName, ArrayType(IntegerType,_), _, _) =>
          df.withColumn(colName, lit("<array<int>>"))
        case StructField(colName, ArrayType(LongType,_), _, _) =>
          df.withColumn(colName, lit("<array<long>>"))
        case StructField(colName, ArrayType(FloatType,_), _, _) =>
          df.withColumn(colName, lit("<array<float>>"))
        case StructField(colName, ArrayType(DoubleType,_), _, _) =>
          df.withColumn(colName, lit("<array<double>>"))
        case StructField(colName, ArrayType(_,_), _, _) =>
          df.withColumn(colName, lit("<array<_>>"))
        case StructField(colName, StructType(_), _, _) => 
          df.withColumn(colName, lit("<struct>"))
        case _ =>
          df
        }
    }
  }

  def showDF(df: DataFrame, title: Option[String]=None, showOpt: Show.Opt=Show.Truncate): MayFail[DataFrame] = MayFail {
    title.map(t => Console.println(Console.CYAN + t + Console.RESET))
    Console.println(Console.CYAN)
    showOpt match {
      case Show.Default     => df.show(5, false)
      case Show.Max(rows)   => df.show(rows, false)
      case Show.Truncate    => df.show(5, true)
      case Show.HideComplex => simplify(df).show(5, false)
    }
    Console.println(Console.RESET)
    df
  }

  def showDFStream(df: DataFrame, title: Option[String]=None): MayFail[DataFrame] = MayFail {
    title.map(t => Console.println(Console.MAGENTA + t + Console.RESET))
    Console.println()
    val q = df.writeStream
      .outputMode("append")
      .format("console")
      .start()
    q.awaitTermination(50)
    Console.println(Console.RESET)
    df
  }

  def showSchema(df: DataFrame): MayFail[DataFrame] =  MayFail {
    df.printSchema
    df
  }
}

object Read {

  def csv(path: String, withHeader: Boolean = true, delimiter: String = ",")
  (implicit spark: SparkSession): MayFail[DataFrame] = {
    import spark.implicits._
    Log.info(s"[IO] Read CSV : ${path}")
    MayFail {
      val df = spark
        .read
        .option("header", withHeader.toString)
        .option("inferSchema", "true")
        .option("delimiter", delimiter)
        .csv(path)
      df
    }
  }
  
  def parquet(path: String) 
  (implicit spark: SparkSession): MayFail[DataFrame] = {
    Log.info(s"[IO] Read parquet : ${path}")
    import spark.implicits._
    MayFail {
      val df = spark
        .read
        .parquet(path)
      df
    }
  }

  def kafkaStream(
    topic: String, 
    serverAddr: String, 
    port: Int = 9092, 
    offset: Option[String] = None, // Offset should be in format : {topicA: -1}
    waitTimeout: Option[Int] = None, // in MS
    colEncoder: ColumnEncoder.Encoder = ColumnEncoder.None)
  (implicit spark: SparkSession): MayFail[DataFrame] = {
    Log.info(s"[IO] Read kafka stream : ${serverAddr}:${port} => topic = ${topic}")
    import spark.implicits._
    MayFail {
      val df = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", s"${serverAddr}:${port}")
        .option("kafka.requests.timeout.ms", waitTimeout.getOrElse(30).toString)
        .option("subscribe", topic)
        .option("startingOffsets", offset.getOrElse("earliest"))
        .load()
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

      colEncoder match {
        case ColumnEncoder.None => df
        case ColumnEncoder.Avro(schema) => df.select(
          from_avro('key, schema).as("key"),
          from_avro('value, schema).as("value")
        )
      }
    }
  }

  def kafka(topic: String, serverAddr: String, port: Int = 9092, colEncoder: ColumnEncoder.Encoder = ColumnEncoder.None)
  (implicit spark: SparkSession): MayFail[DataFrame] = {
    Log.info(s"[IO] Read kafka batch : ${serverAddr}:${port} => topic = ${topic}")
    import spark.implicits._
    MayFail {
      val df = spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", s"${serverAddr}:${port}")
        .option("subscribe", topic)
        .load()
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      
      colEncoder match {
        case ColumnEncoder.None => df
        case ColumnEncoder.Avro(schema) => df.select(
          from_avro('key, schema).as("key"),
          from_avro('value, schema).as("value")
        )
      }
    }
  }
  
  def mongo(serverAddr: String, db: String, collection: String)
  (implicit spark: SparkSession): MayFail[DataFrame] = MayFail {
    spark.read.format("mongo")
      .option("uri", s"mongodb://${serverAddr}/${db}.${collection}")
      .load()
  }

  def dynamo(region: String, serverAddr: String, tb: String)
  (implicit spark: SparkSession): MayFail[DataFrame] = MayFail {
    Amazon.Dynamo.read(region, serverAddr, tb)
  }
}

object Write {
  
  trait Partition
  case object NoPartition extends Partition
  case class PartitionCol(cols: List[String]) extends Partition {
    override def toString = s"ParitionCol(${cols.mkString(", ")})"
  }

  protected def preprocess(df: DataFrame, partition: Partition, overwrite: Boolean = false) = {
    val par = partition match {
      case NoPartition => df.coalesce(1).write
      case PartitionCol(cols) => df.write.partitionBy(cols:_*)
    }
    if (overwrite) 
      par.mode("overwrite")
    else 
      par
  }

  def csv(
    df: DataFrame, 
    path: String,
    partition: Partition = NoPartition,
    delimiter: String = ",",
    overwrite: Boolean = false)
  (implicit spark: SparkSession): MayFail[DataFrame] = MayFail {
    Log.info(s"[IO] Write CSV : ${path}, partitioned with ${partition}")
    import spark.implicits._
    preprocess(df, partition, overwrite)
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", delimiter)
      .csv(path)
    df
  }

  def parquet(
    df: DataFrame, 
    path: String,
    partition: Partition = NoPartition,
    overwrite: Boolean = false)
  (implicit spark: SparkSession): MayFail[DataFrame] = MayFail {
    Log.info(s"[IO] Write parquet : ${path}, partitioned with ${partition}")
    import spark.implicits._
    preprocess(df, partition, overwrite).parquet(path)
    df
  }

  def kafkaStream(
    df: DataFrame, 
    topic: String, 
    serverAddr: String, 
    port: Int = 9092,
    waitTimeout: Option[Int] = None, // in MS
    colEncoder: ColumnEncoder.Encoder = ColumnEncoder.None,
    checkpointLocation: String = "./chk",
    terminationTimeout: Option[Int] = None): MayFail[DataFrame] = MayFail {
    Log.info(s"[IO] Write kafka stream : ${serverAddr}:${port} => topic = ${topic}, checkpointLocation = ${checkpointLocation}")
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
      .option("kafka.fetch.max.wait.ms", waitTimeout.getOrElse(30).toString)
      .option("topic", topic)
      .option("outputMode", "append")
      .option("checkpointLocation", checkpointLocation)
      .start()

    terminationTimeout match {
      case None => q.awaitTermination()
      case Some(t) => q.awaitTermination(t)
    }
    
    df
  }

  def kafka(
    df: DataFrame, 
    topic: String, 
    serverAddr: String, 
    port: Int = 9092,
    colEncoder: ColumnEncoder.Encoder = ColumnEncoder.None): MayFail[DataFrame] = MayFail {
    Log.info(s"[IO] Write kafka batch : ${serverAddr}:${port} => topic = ${topic}")
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
    df
  }

  def streamToFile(
    df: DataFrame,
    fileType: String,
    path: String,
    partition: Partition = NoPartition,
    checkpointLocation: String = "./chk",
    timeout: Option[Int] = None): MayFail[DataFrame] = MayFail {

    Log.info(s"[IO] Write stream of file : ${path}, partitioned by ${partition}")
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
    df
  }

  def mongo(
    df: DataFrame, 
    serverAddr: String, 
    db: String, 
    collection: String): MayFail[DataFrame] = MayFail {
    df.write.format("mongo")
      .mode("append")
      .option("uri", s"mongodb://${serverAddr}/${db}.${collection}")
      .save()
    df
  }

  def dynamo(df: DataFrame, serverAddr: String, tb: String): MayFail[DataFrame] = MayFail {
    ???
  }
}

object Transform {

  def apply(df: DataFrame, f: DataFrame => DataFrame): MayFail[DataFrame] = MayFail {
    f(df)
  }

  def select(df: DataFrame, cols: Seq[String]): MayFail[DataFrame] = MayFail {
    df.select(cols.head, cols.tail:_*)
  }

  def rename(df: DataFrame, map: Map[String, String]): MayFail[DataFrame] = MayFail {
    map.foldLeft(df){ case (df_, pair) =>
      df_.withColumnRenamed(pair._1, pair._2)
    }
  }
}
