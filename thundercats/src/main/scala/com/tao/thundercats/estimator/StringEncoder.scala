package com.tao.thundercats.estimator

import org.apache.spark.ml.feature._
import org.apache.spark.ml.{Estimator, Model, Transformer}
import org.apache.spark.ml.attribute.{Attribute, NominalAttribute}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.{HasHandleInvalid, HasInputCol, HasOutputCol}
import org.apache.spark.ml.util._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions.{log => logNatural, _}
import org.apache.spark.sql.types._
import org.apache.spark.ml.util.MLWriter
import org.apache.spark.SparkException
import scala.reflect.runtime.universe._
import scala.reflect.ClassTag
import org.apache.hadoop.fs.Path

import org.apache.spark.ml.param.shared._

import java.io.File
import sys.process._
import scala.reflect.io.Directory
import scala.util.control.Exception._
import scala.util.Try
import scala.util.hashing.MurmurHash3

import com.tao.thundercats.physical._
import com.tao.thundercats.functional._
import com.tao.thundercats.physical.Implicits._
import com.tao.thundercats.model._

object Implicits {
  val HASH_SEED = 0x8623  
}

trait EncoderMethod 
case object Murmur extends EncoderMethod
case class TFIDF(minFreq: Int = 1) extends EncoderMethod
case object WordVector extends EncoderMethod // REVIEW: Not yet supported

trait TokenMethod {
  def splitDF(ds: Dataset[_], inputCol: String, outputCol: String): DataFrame
  def apply(ds: Dataset[_], inputCol: String, outputCol: String) = 
    splitDF(ds, inputCol, outputCol)
}
case object WhiteSpaceToken extends TokenMethod {
  override def splitDF(ds: Dataset[_], inputCol: String, outputCol: String): DataFrame = {
    ds.withColumn(outputCol, split(col(inputCol), "\\s+"))
  }
}
case class AsianLanguageToken(lang: String) extends TokenMethod {
  override def splitDF(ds: Dataset[_], inputCol: String, outputCol: String): DataFrame = ???
}

trait StringEncoderParams extends Params
with HasInputColExposed
with HasOutputColExposed {
  // final def logScale: Param[Boolean] = new Param[Boolean](this, "logScale", "Boolean indicating whether log scale is used.")
  // final def norm: Param[Boolean] = new Param[Boolean](this, "norm", "Turning on or off normalisation scaler")

  setDefault(inputCol -> "input")
  setDefault(outputCol -> "output")
}

/**
 * A complete string tokeniser and encoder
 */
class StringEncoder(
  method: EncoderMethod=Murmur, 
  tokeniser: TokenMethod=WhiteSpaceToken,
  // REVIEW: with typo correction techniques
  override val uid: String = Identifiable.randomUID("StringEncoder"))
extends Estimator[StringEncoderModel]
with StringEncoderParams
with DefaultParamsWritable {

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)

  override def transformSchema(schema: StructType) = 
    schema.add($(outputCol), ArrayType(DoubleType,true), true)

  def setInputCol(value: String): this.type = set(inputCol, value)
  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def fit(dataset: Dataset[_]): StringEncoderModel = {
    transformSchema(dataset.schema, logging=true)

    method match {
      case Murmur => 
        new StringEncoderModel(MurmurModel, tokeniser)
      case TFIDF(minFreq) =>
        val tf = new HashingTF()
          .setInputCol($(inputCol))
          .setOutputCol($(outputCol))
        val idf = new IDF()
          .setInputCol($(outputCol))
          .setOutputCol($(outputCol))
          .fit(tf.transform(dataset))
        new StringEncoderModel(TFIDFModel(tf, idf), tokeniser)
      case _ => throw new java.util.InvalidPropertiesFormatException(s"Unsupported encoder method : ${method}")
    }
  }
}

private [estimator] trait FittedEncoderModel {
  def transform(dataset: Dataset[_], column: String): DataFrame 
}

case object MurmurModel extends FittedEncoderModel {

  lazy val hashUDF = udf((s: String) => MurmurHash3.stringHash(s, Implicits.HASH_SEED))
  def transform(dataset: Dataset[_], column: String): DataFrame = 
    dataset.withColumn(column, hashUDF(col(column)))
}

case class TFIDFModel(tf: HashingTF, idf: IDFModel) extends FittedEncoderModel {
  def transform(dataset: Dataset[_], column: String): DataFrame = {
    idf.transform(tf.transform(dataset))
  }
}


class StringEncoderModel(
  model: FittedEncoderModel,
  tokenMethod: TokenMethod,
  override val uid: String = Identifiable.randomUID("ScalerModel"))
extends Model[StringEncoderModel] 
with ScalerParams {

  override def copy(extra: ParamMap): StringEncoderModel = {
    val copied = new StringEncoderModel(model, tokenMethod)
        .setInputCol($(inputCol))
        .setOutputCol($(outputCol))
    copyValues(copied, extra).setParent(parent)
  }

  def setInputCol(value: String): this.type = set(inputCol, value)
  def setOutputCol(value: String): this.type = set(outputCol, value)

  def transformAndValidate(schema: StructType): StructType = {
    val inputColumn = $(inputCol)
    val outputColumn = $(outputCol)
    require(schema.map(_.name) contains inputColumn, s"Dataset has to contain the input column : $inputColumn")
    schema.add(StructField(outputColumn, DoubleType, false))
  }

  def transformSchema(schema: StructType): StructType = transformAndValidate(schema)

  def transform(dataset: Dataset[_]): DataFrame = {
    transformAndValidate(dataset.schema)
    model.transform(tokenMethod(dataset, $(inputCol), $(outputCol)), $(outputCol))
  }

}