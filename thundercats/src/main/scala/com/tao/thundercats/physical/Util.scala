package com.tao.thundercats.physical

import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Column,Row}
import org.apache.spark.sql.catalyst.encoders._
import org.apache.spark.sql.{Encoders, Encoder}
import org.apache.spark.sql.avro._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, StructField}

object Implicits {

  implicit class DataFrameOps(val df: DataFrame) extends AnyVal {

    def schemaMap: Map[String, DataType] = {
      df.schema.toList.map{ case StructField(n,m,_,_) => 
        (n,m)
      }.toMap
    }

  }

}