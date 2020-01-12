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
 * Feature engineering pipeline creator (Monad as builder pattern)
 */
object Feature {

  def tfidf(col: String, output: String): Monad[Seq[Transformer]]

  def padArray(cols: Seq[String], maxLength: Option[Int]=None): Monad[Seq[Transformer]]

  def assembly(cols: Seq[String], output: String): Monad[Seq[Transformer]]

}