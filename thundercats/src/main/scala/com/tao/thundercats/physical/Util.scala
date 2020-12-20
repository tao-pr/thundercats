package com.tao.thundercats.physical

import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Column,Row}
import org.apache.spark.sql.catalyst.encoders._
import org.apache.spark.sql.{Encoders, Encoder}
import org.apache.spark.rdd.DoubleRDDFunctions
import org.apache.spark.sql.avro._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.rdd.RDD
import org.apache.spark.ml.{Transformer, PipelineModel}
import org.apache.spark.ml.{Pipeline, Estimator, PipelineStage}

import org.apache.log4j.Logger

import scala.util.Try

import com.tao.thundercats.functional._

object Implicits {

  implicit class DataFrameOps(val df: DataFrame) extends AnyVal {

    def schemaMap: Map[String, DataType] = {
      df.schema.toList.map{ case StructField(n,m,_,_) => (n,m) }.toMap
    }

    // Bind
    def >>(f: DataFrame => DataFrame): MayFail[DataFrame] = MayFail { f(df) }

    def getDoubleRDD(colName: String): RDD[Double] = {
      df.schema.find(_.name == colName).get match {
        case StructField(colName, DoubleType, _, _) => df.rdd.map(_.getAs[Double](colName))
        case StructField(colName, IntegerType, _, _) => 
          df.withColumn(colName, col(colName).cast(DoubleType))
            .rdd.map(_.getAs[Double](colName))
      }
    }

    def sumOfSqrDiff(colA: String, colB: String): Double = {
      val tmpCol = s"${colA}-${colB}"
      val dfDiff = df
        .withColumn(tmpCol, col(colA).cast(DoubleType) - col(colB).cast(DoubleType))
        .withColumn(tmpCol, col(tmpCol)*col(tmpCol))
      val diff = new DoubleRDDFunctions(dfDiff.rdd.map(_.getAs[Double](tmpCol)))
      diff.sum
    }

    def sumOfSqr(colName: String): Double = {
      val x2 = new DoubleRDDFunctions(getDoubleRDD(colName).map{ v => 
        scala.math.pow(v, 2.0)})
      x2.sum
    }
  }

  implicit class DoubleOps(val d: Double) extends AnyVal {
    def sqrt = scala.math.sqrt(d)
  }

}

object Debugger {

  def printPipeline(pipeline: Pipeline): Unit = {
    pipeline.getStages.foreach{stage =>
      if (stage.isInstanceOf[Pipeline]){
        printPipeline(stage.asInstanceOf[Pipeline])
      }
      else Console.println(s"... ${stage.getClass.getName}")
    }
  }

  def printModel(model: PipelineModel): Unit = {
    model.stages.foreach{ trans =>
      if (trans.isInstanceOf[PipelineModel])
        printModel(trans.asInstanceOf[PipelineModel])
      else 
        Console.println(s"... ${trans.getClass.getName}")
    }
  }

  def modelToString(model: PipelineModel, prev: String = ""): String = {
    var str = prev
    model.stages.foreach{ trans =>
      if (trans.isInstanceOf[PipelineModel])
        str = modelToString(trans.asInstanceOf[PipelineModel], str)
      else 
        str += s" => ${trans.getClass.getName}"
    }
    str
  }
}

object Log extends Serializable {
  @transient lazy val log = Logger.getLogger("com.tao.thundercats")
  def info(s: String) = log.info(s)
  def error(s: String) = log.error(s)
  def debug(s: String) = log.debug(s)
}