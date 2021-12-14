package com.tao.thundercats.physical

import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.SparkSession

object Amazon {
  object Dynamo {
    def read(region: String, serverAddr: String, tb: String, query: Option[String]=None)
    (implicit spark: SparkSession): DataFrame = {
      ???
    } 
  }
}