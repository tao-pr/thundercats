package com.tao.thundercats.physical

import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Column,Row}
import org.apache.spark.sql.catalyst.encoders._
import org.apache.spark.sql.{Encoders, Encoder}
import org.apache.spark.sql.avro._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.io.File
import sys.process._
import scala.reflect.io.Directory
import scala.util.Try

import com.tao.thundercats.physical._
import com.tao.thundercats.functional._

object Join {

  private [physical] trait Joiner
  case class On(cols: Seq[String]) extends Joiner
  case class With(w: Column) extends Joiner

  private def join(strategy: String)(df1: DataFrame, df2: DataFrame, on: Joiner): MayFail[DataFrame] = on match {
    case On(on) => MayFail(df1.join(df2, on, strategy))
    case With(w) => MayFail(df1.join(df2, w, strategy))
  }

  def left(df1: DataFrame, df2: DataFrame, on: Joiner): MayFail[DataFrame] = join("left")(df1, df2, on)

  def inner(df1: DataFrame, df2: DataFrame, on: Joiner): MayFail[DataFrame] = join("inner")(df1, df2, on)

  def outer(df1: DataFrame, df2: DataFrame, on: Joiner): MayFail[DataFrame] = join("outer")(df1, df2, on)

  /**
   * Broadcast the right tiny dataframe, join with left join
   */
  def broadcast(
    dfBig: DataFrame, 
    dfTiny: DataFrame, 
    on: Seq[String], 
    rightColumns: Seq[String])
  :MayFail[DataFrame] = MayFail {
    
    import Implicits._
    import dfBig.sqlContext.implicits._

    val sc = dfBig.sqlContext.sparkContext

    val allRightCols = (on.toSet union rightColumns.toSet).toSeq
    val right = dfTiny.select(allRightCols.head, allRightCols.tail:_*)
    val rightSchemaMap = sc.broadcast(right.schemaMap).value

    val toKey = (n: Row) => {
      on.map{ c => 
        (rightSchemaMap(c) match {
          case IntegerType => n.getAs[Int](c)
          case DoubleType => n.getAs[Double](c)
          case StringType => n.getAs[String](c)
          case BooleanType => n.getAs[Boolean](c)
          case ArrayType(t,_) => t match {
            case IntegerType => n.getAs[Seq[Int]](c)
            case DoubleType => n.getAs[Seq[Double]](c)
            case StringType => n.getAs[Seq[String]](c)
            case BooleanType => n.getAs[Seq[Boolean]](c)
          }
        }).toString
      }.mkString("--")
    }

    // Collect the right dataframe (keyed) as a map
    val rightMap = sc.broadcast(dfTiny
      .select(allRightCols.head, allRightCols.tail:_*)
      .rdd
      .keyBy(toKey)
      .map{ case (k,row) => (k, Seq(row.toSeq.drop(on.size))) }
      .reduceByKey{ (a,n) => n ++ a }
      .collectAsMap).value

    // Joining task to be executed at partition level
    // One left row can match multiple right rows
    val join = (n: Row) => { // Returns Seq[Row]
      rightMap.get(toKey(n)).map{_.map(row => Row.fromSeq(n.toSeq ++ row))}.getOrElse(Nil)
    }

    val rdd = dfBig
      .rdd
      .mapPartitions(_.flatMap(join))

    val joinedSchema = StructType(dfBig.schema.toList ++ right.schema.toList.drop(on.size))

    new org.apache.spark.sql.SQLContext(sc).createDataFrame(rdd, joinedSchema)
  }

}

object Group {

  private [physical] trait Strategy
  // [[Map]] needs Hive metastore
  case class Map(m: scala.collection.immutable.Map[String, String]) extends Strategy
  // [[Agg]] works without Hive metastore
  case class Agg(f: Seq[Column]) extends Strategy

  object Map {
    def apply(m: (String, String)*): Strategy = {
      Map(scala.collection.immutable.Map(m:_*))
    }
  }

  def agg(df: DataFrame, by: Seq[Column], agg: Strategy): MayFail[DataFrame] = MayFail {
    val g = df.groupBy(by:_*)
    agg match {
      case Map(m) => g.agg(m)
      case Agg(f) => g.agg(f.head, f.tail:_*)
    }
  }

}

object Filter {
  def where(df: DataFrame, cond: Column): MayFail[DataFrame] = MayFail(df.filter(cond))
  
  /**
   * Remove nulls
   */
  def na(df: DataFrame, cols: Seq[String]): MayFail[DataFrame] = MayFail{
    val cond = cols.map(c => col(c).isNull).reduce(_ || _)
    df.where(not(cond))
  }

  /**
   * Remove out-of-bound values
   */
  def byRange[T](df: DataFrame, column: String, bound: Tuple2[T,T]): MayFail[DataFrame] = MayFail {
    val (lb, ub) = bound
    df.where(col(column) >= lb && col(column) <= ub)
  }
}

object Order {
  def by(df: DataFrame, cols: Seq[String]): MayFail[DataFrame] = MayFail {
    df.orderBy(cols.head, cols.tail:_*)
  }
}

object F {
  def addCol(df: DataFrame, colName: String, c: Column): MayFail[DataFrame] = MayFail {
    df.withColumn(colName, c)
  }

  def lift(df: DataFrame): MayFail[DataFrame] = MayFail { df }
}

object Optimise {
  /**
   * Lineage optimisers
   */ 

  def snapshot(df: DataFrame, tempDir: String)
  (implicit spark: SparkSession): MayFail[DataFrame] = {
    val tempFile = s"$tempDir/${java.util.UUID.randomUUID}.parquet"
    for {
      _   <- Write.parquet(df, tempFile)
      df_ <- Read.parquet(tempFile)
    } yield df_
  }

  def materialise(df: DataFrame): MayFail[DataFrame] = MayFail {
    df.cache
    df.count
    df
  }

  def repar(df: DataFrame, num: Int) = MayFail {
    if (num<=1) df.coalesce(1)
    else df.repartition(num)
  }

  def repar(df: DataFrame, cols: Seq[Column]) = MayFail {
    df.repartition(cols:_*)
  }
}

