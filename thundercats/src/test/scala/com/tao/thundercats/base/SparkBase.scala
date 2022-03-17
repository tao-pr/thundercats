package com.tao.thundercats.base

import org.apache.spark.sql.SparkSession
import org.scalatest._
import org.scalatest.funspec.AnyFunSpec

trait SparkTestInstance extends AnyFunSpec with BeforeAndAfterAll {
  def name: String = "test"
  private def getSparkInstance = {
    val instance = SparkSession.builder
      .master("local")
      .appName(name)
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.executor.instances", "1")
      .getOrCreate()

    instance.sparkContext.setLogLevel("ERROR")
    instance
  }

  lazy val spark = getSparkInstance

  implicit def sparkDef = spark

  override def afterAll() {
    try {
      println(Console.MAGENTA + "Tearing down the suite" + Console.RESET)
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
