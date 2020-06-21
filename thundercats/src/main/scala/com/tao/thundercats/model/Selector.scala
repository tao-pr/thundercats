package com.tao.thundercats.model

import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Column,Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import org.apache.spark.ml.{Transformer, PipelineModel}
import org.apache.spark.ml.{Pipeline, Estimator, PipelineStage}
import org.apache.spark.ml.param._

import com.tao.thundercats.physical._
import com.tao.thundercats.functional._
import com.tao.thundercats.physical.Implicits._
import com.tao.thundercats.estimator._
import com.tao.thundercats.evaluation._


/**
 * Generates a lot of specimens
 */
trait SpecimenGenerator {

  /**
   * Collect numerical columns from the given dataframe, vectors are not included
   */
  protected def numericalCols(df: DataFrame, ignoredCols: Seq[String] = Nil): List[String] = {
    df.schema.toList.collect{ 
      case StructField(p,DoubleType,_,_) if (!ignoredCols.contains(p)) => p
      case StructField(p,IntegerType,_,_) if (!ignoredCols.contains(p))=> p
      case StructField(p,FloatType,_,_) if (!ignoredCols.contains(p)) => p
    }
  }

  // Generate multiple [[Specimen]]
  def genIter(pipe: Pipeline, df: DataFrame, outputCol: String, labelCol: String): Iterable[Specimen]
}

class FeatureAssemblyGenerator(
  minFeatureCombination: Int,
  maxFeatureCombination: Int = Int.MaxValue,
  ignoreCols: Seq[String] = Nil
) 
extends SpecimenGenerator {
  override def genIter(pipe: Pipeline, df: DataFrame, outputCol: String, labelCol: String): Iterable[Specimen] = {
    assert(minFeatureCombination > 0)
    assert(minFeatureCombination <= maxFeatureCombination)
    val featCols = numericalCols(df, ignoreCols)
    val numMaxComb = scala.math.min(maxFeatureCombination, featCols.size)
    (minFeatureCombination to numMaxComb).flatMap{
      numFeat => featCols.combinations(numFeat).map{
        features => FeatureModelDesign(outputCol, labelCol, pipe).toSpecimen(
          if (numFeat==1) Feature(features.head)
          else AssemblyFeature(features),
          df
        )
      }
    }
  }
}


