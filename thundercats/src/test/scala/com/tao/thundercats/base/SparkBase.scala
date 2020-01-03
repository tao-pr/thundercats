package com.tao.thundercats.base

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SparkSession, SQLContext, SQLImplicits}
import org.apache.spark.util.Utils

import org.scalatest._

trait SparkTestInstance extends FunSpec with BeforeAndAfterAll {
  def name: String = "test"
  private def getSparkInstance = {
    val instance = SparkSession.builder
      .master("local")
      .appName(name)
      .getOrCreate()

    instance.sparkContext.setLogLevel("ERROR")
    instance
  }

  lazy val spark = getSparkInstance

  implicit def sparkDef = spark

  override def afterAll() {
    try {
      SparkSession.clearActiveSession()
      if (spark != null) {
        Console.println("Stopping Spark instance")
        spark.stop()
      }
    } finally {
      super.afterAll()
    }
  }
}

trait SparkStreamTestInstance extends SparkTestInstance