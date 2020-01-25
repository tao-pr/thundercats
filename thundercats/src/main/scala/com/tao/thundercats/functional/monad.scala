package com.tao.thundercats.functional

import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Column,Row}
import org.apache.spark.sql.catalyst.encoders._
import org.apache.spark.sql.{Encoders, Encoder}
import org.apache.spark.sql.avro._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.util.{Try, Success, Failure}

/**
 * Alternative to [[Option]]
 * but carrying error message in case of failure
 */
trait MayFail[A] {
  def map(f: A => A): MayFail[A]
  def flatMap(g: A => MayFail[A]): MayFail[A]
  def get: A
  def getError: Option[String]
  def isFailing: Boolean
}

object MayFail {
  def apply[R](a: => R): MayFail[R] = Try { a } match {
    case Success(b) => Ok(b)
    case Failure(e) => Fail(e.getMessage.toString)
  }
}

case class Fail[A](errorMessage: String) extends MayFail[A] {
  override def map(f: A => A): MayFail[A] = this
  override def flatMap(g: A => MayFail[A]): MayFail[A] = this
  override def get: A = throw new java.util.NoSuchElementException("No value resolved")
  override def isFailing = true
  override def getError: Option[String] = Some(errorMessage)
}
case class IgnorableFail[A](errorMessage: String, data: A) extends MayFail[A] {
  override def map(f: A => A): MayFail[A] = IgnorableFail(errorMessage, f(data))
  override def flatMap(g: A => MayFail[A]): MayFail[A] = this
  override def get: A = data
  override def isFailing = true
  override def getError: Option[String] = Some(errorMessage)
}
case class Ok[A](data: A) extends MayFail[A] {
  override def map(f: A => A): MayFail[A] = Ok(f(data))
  override def flatMap(g: A => MayFail[A]): MayFail[A] = g(data)
  override def get: A = data
  override def isFailing = false
  override def getError: Option[String] = None
}