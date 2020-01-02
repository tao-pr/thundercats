package com.tao.thundercats.monad

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Column,Row}
import org.apache.spark.sql.catalyst.encoders._
import org.apache.spark.sql.{Encoders, Encoder}
import org.apache.spark.sql.avro._
import org.apache.spark.sql.functions._

/**
 * Generic Monad of type [[A]]
 */
trait M[A] {
  def flatMap(f: A => M[A]): M[A]
  def unit(u: A): M[A]
}
