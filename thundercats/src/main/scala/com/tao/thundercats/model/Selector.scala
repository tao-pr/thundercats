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
  protected def numericalCols(df: DataFrame, ignoredCols: List[String] = Nil): List[String] = {
    df.schema.toList.collect{ 
      case StructField(p,DoubleType,_,_) if (!ignoredCols.contains(p)) => p
      case StructField(p,IntegerType,_,_) if (!ignoredCols.contains(p))=> p
      case StructField(p,FloatType,_,_) if (!ignoredCols.contains(p)) => p
    }
  }

  // Generate feature combinations
  def genCombinations(pipe: Pipeline, df: DataFrame): Iterable[FeatureColumn]
}

/**
 * Generating a combination of features. The size of the feature vector 
 * varies within the given range, exclusion of some columns are also possible.
 */
class FeatureAssemblyGenerator(
  minFeatureCombination: Int,
  maxFeatureCombination: Int = Int.MaxValue,
  ignoreCols: List[String] = Nil
) 
extends SpecimenGenerator {
  override def genCombinations(pipe: Pipeline, df: DataFrame): Iterable[FeatureColumn] = {
    assert(minFeatureCombination > 0)
    assert(minFeatureCombination <= maxFeatureCombination)
    val featCols = numericalCols(df, ignoreCols)
    val numMaxComb = scala.math.min(maxFeatureCombination, featCols.size)
    (minFeatureCombination to numMaxComb).flatMap{
      numFeat => featCols.combinations(numFeat).map{
        features => AssemblyFeature(features)
      }
    }
  }
}


