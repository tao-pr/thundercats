package com.tao.thundercats.physical

import com.tao.thundercats.functional._
import org.apache.log4j.Logger
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.rdd.{DoubleRDDFunctions, RDD}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

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
            .rdd.map {
            _.getAs[Double](colName)
          }
      }
    }

    def sumOfSqrDiff(colA: String, colB: String): Double = {
      val tmpCol = s"$colA-$colB"
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
    def sqrt: Double = scala.math.sqrt(d)
  }

}

object Debugger {

  def printPipeline(pipeline: Pipeline): Unit = {
    pipeline.getStages.foreach {
      case pipe: Pipeline =>
        printPipeline(pipe)
      case stage => Console.println(s"... ${stage.getClass.getName}")
    }
  }

  def printModel(model: PipelineModel): Unit = {
    model.stages.foreach {
      case pipelineModel: PipelineModel => printModel(pipelineModel)
      case trans => Console.println(s"... ${trans.getClass.getName}")
    }
  }

  def modelToString(model: PipelineModel, prev: String = ""): String = {
    var str = prev
    model.stages.foreach {
      case pipelineModel: PipelineModel => str = modelToString(pipelineModel, str)
      case trans => str += s" => ${trans.getClass.getName}"
    }
    str
  }
}

object Log extends Serializable {
  @transient lazy val log: Logger = Logger.getLogger("com.tao.thundercats")
  def info(s: String): Unit = log.info(s)
  def error(s: String): Unit = log.error(s)
  def debug(s: String): Unit = log.debug(s)
}