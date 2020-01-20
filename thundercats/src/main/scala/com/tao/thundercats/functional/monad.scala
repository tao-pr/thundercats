package com.tao.thundercats.functional

import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Column,Row}
import org.apache.spark.sql.catalyst.encoders._
import org.apache.spark.sql.{Encoders, Encoder}
import org.apache.spark.sql.avro._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

trait Monad[A] {
  def map(f: A => A): Monad[A]
  def flatMap(g: A => Monad[A]): Monad[A]
}

/**
 * Alternative to [[Option]]
 */
trait MayFail[A] extends Monad[A]

case class Fail[A](errorMessage: String) extends MayFail[A] {
  override def map(f: A => A): Monad[A] = this
  override def flatMap(g: A => Monad[A]): Monad[A] = this
}
case class IgnorableFail[A](errorMessage: String, data: A) extends MayFail[A] {
  override def map(f: A => A): Monad[A] = IgnorableFail(errorMessage, f(data))
  override def flatMap(g: A => Monad[A]): Monad[A] = this
}
case class Success[A](data: A) extends MayFail[A] {
  override def map(f: A => A): Monad[A] = Success(f(data))
  override def flatMap(g: A => Monad[A]): Monad[A] = g(data)
}