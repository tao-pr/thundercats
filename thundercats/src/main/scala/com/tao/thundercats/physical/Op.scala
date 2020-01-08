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
  def broadcast(dfBig: DataFrame, dfTiny: DataFrame, on: Seq[String], rightColumns: Seq[String]) = {
    
    import Implicits._

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
      .collectAsMap).value

    // Joining task to be executed at partition level
    val join = (n: Row) => { // Returns Option[Row]
      val key = toKey(n)
      rightMap.get(key).map { rightRow => 
        Row.fromSeq(n.toSeq ++ rightRow.toSeq.drop(on.size))
      }
    }

    val rdd = dfBig
      .rdd
      .mapPartitions(_.map(join))
      .collect { case Some(row) => row }

    val joinedSchema = StructType(dfBig.schema.toList ++ right.schema.toList)

    new org.apache.spark.sql.SQLContext(sc).createDataFrame(rdd, joinedSchema)
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
