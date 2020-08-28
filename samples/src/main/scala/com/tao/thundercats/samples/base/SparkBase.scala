package com.tao.thundercats.samples.base

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SparkSession, SQLContext, SQLImplicits}
import org.apache.spark.util.Utils

/**
 * Spark instance goes here
 */
trait SparkBase {

  def name: String = "sparkbase"
  def cluster: String = "local"

  private def getSparkInstance = {
    val instance = SparkSession.builder
      .master(cluster)
      .appName(name)
      .getOrCreate()

    instance.sparkContext.setLogLevel("ERROR")
    instance
  }

  lazy val spark = getSparkInstance

  implicit def sparkDef = spark

  def end() {
    try {
      println(Console.MAGENTA + "Tearing down the suite" + Console.RESET)
      SparkSession.clearActiveSession()
      if (spark != null) {
        Console.println("Stopping Spark instance")
        spark.stop()
      }
    } finally {
      Console.println("Spark instance shutdown ...")
    }
  }


}