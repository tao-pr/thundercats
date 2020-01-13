package com.tao.thundercats.physical

import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Column,Row}
import org.apache.spark.sql.catalyst.encoders._
import org.apache.spark.sql.{Encoders, Encoder}
import org.apache.spark.sql.avro._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.ml.Transformer

import java.io.File
import sys.process._
import scala.reflect.io.Directory
import scala.util.Try

import com.tao.thundercats.physical._
import com.tao.thundercats.functional._

/**
 * State monad holding sequence of feature transformers [[A]]
 */
trait FeatureMonad[A] extends Monad[Seq[A]] {
  def + (then: FeatureMonad[A]): FeatureMonad[A]
}

/**
 * Feature engineering pipeline creator (Monad as builder pattern)
 */
object Feature {

  case object Nil extends FeatureMonad[Transformer] {
    override def + (then: FeatureMonad[Transformer]): FeatureMonad[Transformer] =
      then match {
        case Nil => Nil
        case Recipe(ns) => Recipe(ns)
      }

    override def map(f: A => A): A = Nil
    override def flatMap(g: A => Monad[A]): Monad[A] = Nil
  }

  case class Recipe(ns: Seq[Transformer]) extends FeatureMonad[Transformer] {
    override def + (then: FeatureMonad[Transformer]): FeatureMonad[Transformer] =
      then match {
        case Nil => this
        case Recipe(ns_) => Recipe(ns ++ ns_)
      }

    override def map(f: A => A): A = Recipe(ns.map(f))

    override def flatMap(g: A => Monad[A]): Monad[A] = {
      ns.map(g).reduce(_ + _)
    }
  }

  def padArray(cols: Seq[String], maxLength: Option[Int]=None, padValue: Double=0): FeatureMonad[Transformer] = ???

  def assemblyVector(cols: Seq[String], output: String): FeatureMonad[Transformer] = ???

}

object TextFeature {}
