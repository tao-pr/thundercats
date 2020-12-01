package com.tao.thundercats.estimator

import org.apache.spark.rdd.RDD
import org.apache.spark.ml.feature._
import org.apache.spark.ml.{Estimator, Model, Transformer}
import org.apache.spark.ml.attribute.{Attribute, NominalAttribute}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.ml.linalg.{Vector, Vectors, VectorUDT}
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.util.MLWriter
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions.{log => logNatural, _}
import org.apache.spark.SparkException
import scala.reflect.runtime.universe._
import scala.reflect.ClassTag
import org.apache.hadoop.fs.Path

import java.io.File
import sys.process._
import scala.reflect.io.Directory
import scala.util.control.Exception._
import scala.util.Try
import scala.util.hashing.MurmurHash3
import collection.immutable.SortedSet

import com.tao.thundercats.physical._
import com.tao.thundercats.functional._
import com.tao.thundercats.physical.Implicits._
import com.tao.thundercats.model._

object PREDEF {
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
  
  val tempCols: Param[List[String]] = new Param[List[String]](
    this, "tempCols", "A list of temporary columns to be dropped before returning")

  setDefault(inputCol -> "input")
  setDefault(outputCol -> "output")
  setDefault(tempCols -> List.empty)
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
    schema.add($(outputCol), VectorType, true)

  def setInputCol(value: String): this.type = set(inputCol, value)
  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def fit(dataset: Dataset[_]): StringEncoderModel = {
    transformSchema(dataset.schema, logging=true)

    method match {
      case Murmur => 
        Log.info(s"Murmur string encoder : input col = ${$(inputCol)}, output col = ${$(outputCol)}")
        val dfTokenised = tokeniser.splitDF(
          dataset.toDF, $(inputCol), $(inputCol) + StringEncoderModel.TOKEN_SUFFIX)
        val hashSpace = MurmurModel.toSortedSet(
          dfTokenised, $(inputCol) + StringEncoderModel.TOKEN_SUFFIX)
        new StringEncoderModel(MurmurModel(hashSpace, $(outputCol)), tokeniser)
          .setInputCol($(inputCol))
          .setOutputCol($(outputCol))
      case TFIDF(minFreq) =>
        val TEMP_TF_SUFFIX = "___tf"
        val tf = new HashingTF()
          .setInputCol($(inputCol) + StringEncoderModel.TOKEN_SUFFIX)
          .setOutputCol($(inputCol) + TEMP_TF_SUFFIX)
        Log.info(s"TFIDFModel : Tokenising col = ${$(inputCol)}")
        val splitDf = tokeniser.splitDF(
          dataset.toDF,
          $(inputCol), 
          $(inputCol) + StringEncoderModel.TOKEN_SUFFIX)
        Log.debug(s"TFIDFModel : Fitting TF component")
        val tfDf = tf.transform(splitDf)
        Log.debug(s"TFIDFModel : Fitting IDF component, output col = ${$(outputCol)}")
        val idf = new IDF()
          .setInputCol($(inputCol) + TEMP_TF_SUFFIX)
          .setOutputCol($(outputCol))
          .fit(tfDf)
        Log.info(s"TFIDFModel : Composing the model")
        new StringEncoderModel(TFIDFModel(tf, idf), tokeniser)
          .setInputCol($(inputCol))
          .setOutputCol($(outputCol))
          .setTempCols(($(inputCol) + TEMP_TF_SUFFIX) :: StringEncoderModel.TOKEN_SUFFIX :: Nil)
      case _ => throw new java.util.InvalidPropertiesFormatException(s"Unsupported encoder method : ${method}")
    }
  }
}

private [estimator] trait FittedEncoderModel {
  def transform(dataset: Dataset[_], column: String): DataFrame 
}

case class MurmurModel(hashSet: SortedSet[Int], outputCol: String) extends FittedEncoderModel {

  // REVIEW: Space reduction by truncating lower frequency of words
  // REVIEW: output as sparse vector
  lazy val hashSpace = hashSet.toList.zipWithIndex.toMap
  lazy val hashUDF = udf((seq: Seq[String]) => {
    // Encode string array into hash array
    val hashArray = seq.map(MurmurHash3.stringHash(_, PREDEF.HASH_SEED))
    // Convert to space vector
    Vectors.dense(hashSpace.map{ case (v,index) => 
      hashArray.count(_ == v).toDouble }.toArray)
  })

  def transform(dataset: Dataset[_], column: String): DataFrame = {
    Log.info(s"MurmurModel : Transforming input col = ${column}, output col = ${outputCol}")
    // Encode string array into hash space vector
    dataset.withColumn(outputCol, hashUDF(col(column)))
  }
}

object MurmurModel {
  
  lazy val encodeSingle = udf((s: String) => MurmurHash3.stringHash(s, PREDEF.HASH_SEED))

  def toSortedSet(df: DataFrame, inputCol: String): SortedSet[Int] = {
    val uniqValues = df.withColumn(inputCol, explode(col(inputCol)))
      .withColumn(inputCol, encodeSingle(col(inputCol)))
      .select(inputCol)
      .distinct
      .rdd.map(_.getAs[Int](inputCol))
      .collect
      .toList
    SortedSet(uniqValues:_*)
  }
}

case class TFIDFModel(tf: HashingTF, idf: IDFModel) extends FittedEncoderModel {
  def transform(dataset: Dataset[_], column: String): DataFrame = {
    Log.info(s"TFIDFModel : Transforming input col = ${column}")
    idf.transform(tf.transform(dataset))
  }
}

object StringEncoderModel {
  val TOKEN_SUFFIX = "__token"
}


class StringEncoderModel(
  model: FittedEncoderModel,
  tokenMethod: TokenMethod,
  override val uid: String = Identifiable.randomUID("StringEncoderModel"))
extends Model[StringEncoderModel] 
with StringEncoderParams {

  override def copy(extra: ParamMap): StringEncoderModel = {
    val copied = new StringEncoderModel(model, tokenMethod)
        .setInputCol($(inputCol))
        .setOutputCol($(outputCol))
    copyValues(copied, extra).setParent(parent)
  }

  def setInputCol(value: String): this.type = set(inputCol, value)
  def setOutputCol(value: String): this.type = set(outputCol, value)

  // Temp cols will be removed after transformation
  def setTempCols(value: List[String]): this.type = set(tempCols, value)

  def transformAndValidate(schema: StructType): StructType = {
    val inputColumn = $(inputCol)
    val outputColumn = $(outputCol)
    val tempColumns = $(tempCols)
    require(schema.map(_.name) contains inputColumn, s"Dataset has to contain the input column : $inputColumn")
    schema.add(StructField(outputColumn, VectorType, false))
  }

  def transformSchema(schema: StructType): StructType = transformAndValidate(schema)

  def transform(dataset: Dataset[_]): DataFrame = {
    transformAndValidate(dataset.schema)
    Log.info(s"Transforming StringEncoderModel, method : ${tokenMethod}")
    val df = model.transform(
      tokenMethod(dataset.toDF, $(inputCol), $(inputCol) + StringEncoderModel.TOKEN_SUFFIX), 
      $(inputCol) + StringEncoderModel.TOKEN_SUFFIX)
    
    $(tempCols)
      .foldLeft(df){ case(a,b) => a.drop(b) }
      .drop($(inputCol) + StringEncoderModel.TOKEN_SUFFIX)
  }

}