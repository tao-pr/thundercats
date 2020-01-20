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

object Join {

  private [physical] trait Joiner
  case class On(cols: Seq[String]) extends Joiner
  case class With(w: Column) extends Joiner

  private def join(strategy: String)(df1: DataFrame, df2: DataFrame, on: Joiner): Option[DataFrame] = on match {
    case On(on) => Some(df1.join(df2, on, strategy))
    case With(w) => Some(df1.join(df2, w, strategy))
  }

  def left(df1: DataFrame, df2: DataFrame, on: Joiner): Option[DataFrame] = join("left")(df1, df2, on)

  def inner(df1: DataFrame, df2: DataFrame, on: Joiner): Option[DataFrame] = join("inner")(df1, df2, on)

  def outer(df1: DataFrame, df2: DataFrame, on: Joiner): Option[DataFrame] = join("outer")(df1, df2, on)

  /**
   * Broadcast the right tiny dataframe, join with left join
   */
  def broadcast(
    dfBig: DataFrame, 
    dfTiny: DataFrame, 
    on: Seq[String], 
    rightColumns: Seq[String])
  :Option[DataFrame] = {
    
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

    Some(new org.apache.spark.sql.SQLContext(sc).createDataFrame(rdd, joinedSchema))
  }

}

object Group {

  private [physical] trait Strategy
  case class Map(m: scala.collection.immutable.Map[String, String]) extends Strategy
  case class Agg(f: Seq[Column]) extends Strategy

  object Map {
    def apply(m: (String, String)*): Strategy = {
      Map(scala.collection.immutable.Map(m:_*))
    }
  }

  def agg(df: DataFrame, by: Seq[Column], agg: Strategy): Option[DataFrame] = {
    val g = df.groupBy(by:_*)
    Some(agg match {
      case Map(m) => g.agg(m)
      case Agg(f) => g.agg(f.head, f.tail:_*)
    })
  }

}

object Filter {
  def where(df: DataFrame, cond: Column): Option[DataFrame] = Some(df.where(cond))
}

object F {
  def addColumn(df: DataFrame, colName: String, c: Column): Option[DataFrame] = {
    Some(df.withColumn(colName, c))
  }
}

