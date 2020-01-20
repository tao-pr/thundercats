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

    override def map(f: Seq[Transformer] => Seq[Transformer]): 
      Monad[Seq[Transformer]] = Nil

    override def flatMap(g: Seq[Transformer] => Monad[Seq[Transformer]]): 
      Monad[Seq[Transformer]] = Nil                                                                      
  }

  case class Recipe(ns: Seq[Transformer]) extends FeatureMonad[Transformer] {
    override def + (then: FeatureMonad[Transformer]): FeatureMonad[Transformer] =
      then match {
        case Nil => this
        case Recipe(ns_) => Recipe(ns ++ ns_)
      }

    override def map(f: Seq[Transformer] => Seq[Transformer]): Monad[Seq[Transformer]] = 
      Recipe(f(ns))

    override def flatMap(g: Seq[Transformer] => Monad[Seq[Transformer]]): Monad[Seq[Transformer]] = {
      g(ns)
    }
  }

  /**
   * Make array column consistent on length
   */
  def padArray(
    cols: Seq[String], 
    maxLength: Option[Int]=None, 
    padValue: Double=0): FeatureMonad[Transformer] = {
    ???
  }

  /**
   * Assembly multiple double or array of double columns into one array
   */
  def assemblyVector(
    cols: Seq[String], 
    output: String): FeatureMonad[Transformer] = {
    ???
  }

}

object TextFeature {

  def encodeString(
    cols: Seq[String],
    nullFill: String=""): FeatureMonad[Transformer] = {
    ???
  }

}
