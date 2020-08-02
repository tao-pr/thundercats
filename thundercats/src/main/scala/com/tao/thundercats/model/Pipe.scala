package com.tao.thundercats.model

import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Column,Row}
import org.apache.spark.sql.catalyst.encoders._
import org.apache.spark.sql.{Encoders, Encoder}
import org.apache.spark.sql.avro._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import org.apache.spark.ml.param.shared.HasInputCol
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer, VectorAssembler}
import org.apache.spark.ml.{Transformer, PipelineModel}
import org.apache.spark.ml.{Pipeline, Estimator, PipelineStage}
import org.apache.spark.ml.tuning.CrossValidatorModel

import java.io.File
import sys.process._
import scala.reflect.io.Directory
import scala.util.Try

import com.tao.thundercats.physical._
import com.tao.thundercats.functional._
import com.tao.thundercats.physical.Implicits._

object Pipe {

  def join(pipes: Pipeline*): MayFail[Pipeline] = MayFail {
    new Pipeline().setStages(pipes.toArray)
  }

  def load(filePath: String): MayFail[PipelineModel] = MayFail {
    PipelineModel.load(filePath)
  }

  def save(filePath: String, pipe: PipelineModel): MayFail[PipelineModel] = MayFail {
    pipe.save(filePath)
    pipe
  }

  def estimator(pipe: Pipeline): MayFail[Pipeline] = MayFail {
    pipe.getStages.collect{ case p: Estimator[_] => new Pipeline().setStages(Array(p)) }.last
  }

  def fittedEstimator(pipelineModel: PipelineModel): MayFail[Transformer] = {
    if (pipelineModel.stages.last.isInstanceOf[PipelineModel]) {
      fittedEstimator(pipelineModel.stages.last.asInstanceOf[PipelineModel])
    }
    else Ok(pipelineModel.stages.last) // as [[Transformer]]
  }

  def withoutEstimator(pipe: Pipeline): MayFail[Pipeline] = MayFail {
    new Pipeline().setStages(pipe.getStages.collect{ case t: Transformer => t })
  }

  def add(pipe: Pipeline, s: PipelineStage): MayFail[Pipeline] = MayFail {
    new Pipeline().setStages(pipe.getStages :+ s)
  }

  def prepend(pipe: Pipeline, s: PipelineStage): MayFail[Pipeline] = MayFail {
    new Pipeline().setStages(s +: pipe.getStages)
  }

  /**
   * Override input col of the estimator inside the pipeline (if any)
   */
  def setInputCol(pipe: Pipeline, inputCol: String): MayFail[Pipeline] = MayFail {
    // Assume the last stage is always the estimator
    val featureStages = pipe.getStages.dropRight(1)
    val estimator = pipe.getStages.last match {
      case c:HasInputCol => c.set(c.inputCol, inputCol)
      case any => any
    }
    new Pipeline().setStages(featureStages :+ estimator)
  }

  /**
   * Override output col of the estimator inside the pipeline (if any)
   */
  def setOutputCol(pipe: Pipeline, inputCol: String): MayFail[Pipeline] = MayFail {
    ???
  }

  /**
   * Override label col of the estimator inside the pipeline (if any)
   */
  def setLabelCol(pipe: Pipeline, inputCol: String): MayFail[Pipeline] = MayFail {
    ???
  }
}


