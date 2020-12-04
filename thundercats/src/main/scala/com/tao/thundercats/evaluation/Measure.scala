package com.tao.thundercats.evaluation

import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Column,Row}
import org.apache.spark.sql.catalyst.encoders._
import org.apache.spark.sql.{Encoders, Encoder}
import org.apache.spark.sql.avro._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.rdd.RDD

import org.apache.spark.ml.feature.{HashingTF, Tokenizer, VectorAssembler}
import org.apache.spark.ml.{Transformer, PipelineModel}
import org.apache.spark.ml.{Pipeline, Estimator, PipelineStage}
import org.apache.spark.ml.{Predictor}
import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.ml.param._
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.mllib.stat.correlation.ExposedPearsonCorrelation
import org.apache.spark.rdd.DoubleRDDFunctions
import org.apache.spark.ml.regression._
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics

import breeze.linalg.DenseVector

import com.tao.thundercats.physical._
import com.tao.thundercats.functional._
import com.tao.thundercats.physical.Implicits._
import com.tao.thundercats.estimator._

// Measure for the whole model
trait Measure extends BaseMeasure[Double]{
  override def % (df: DataFrame, specimen: Specimen): MayFail[Double]
  override def isBetter(a: Double, b: Double) = a > b
}

trait RegressionMeasure extends Measure

trait ClassificationMeasure extends Measure {
  // Measure classification as Map of [[threshold -> Score]]
  def %% (df: DataFrame, specimen: Specimen): MayFail[Map[Double,Double]] = {
    %(df, specimen).map{ v => Map(Double.MinValue -> v) }
  }

  def pred(df: DataFrame, specimen: Specimen): MayFail[RDD[(Double,Double)]] = 
    if (!df.columns.contains(specimen.labelCol)){
      Fail(s"Unable to run RegressionMeasure, missing label column (${specimen.labelCol})")
    }
    else MayFail {
      // Generate a sequence of (pred, label)
      df.withColumn(specimen.outputCol, col(specimen.outputCol).cast(DoubleType))
        .withColumn(specimen.labelCol, col(specimen.labelCol).cast(DoubleType))
        .rdd.map{ row => 
          val pre = row.getAs[Double](specimen.outputCol)
          val lbl = row.getAs[Double](specimen.labelCol)
          (pre, lbl)
        }.cache
    }
}

trait ClusterMeasure extends Measure 

/**
 * Calculate fitting error between real label and predicted output.
 * Metric: Root mean square error
 */
case object RMSE
extends RegressionMeasure
with ClassificationMeasure {
  override def % (df: DataFrame, specimen: Specimen): MayFail[Double] = MayFail {
    import specimen._
    val agg = new DoubleRDDFunctions(df
      .withColumn("sqe", pow(col(outputCol) - col(labelCol), 2.0))
      .rdd.map(_.getAs[Double]("sqe")))

    agg.mean.sqrt
  }

  override def isBetter(a: Double, b: Double) = a < b
}

/**
 * Calculate fitting error between real label and predicted output.
 * Metric: Mean absolute error
 */
case object MAE 
extends RegressionMeasure 
with ClassificationMeasure {
  override def % (df: DataFrame, specimen: Specimen): MayFail[Double] = MayFail {
    import specimen._
    val agg = new DoubleRDDFunctions(df
      .withColumn("mae", abs(col(outputCol) - col(labelCol)))
      .rdd.map(_.getAs[Double]("mae")))
    agg.mean
  }

  override def isBetter(a: Double, b: Double) = a < b
}

/**
 * Mean percentage error
 */
case object MPE
extends RegressionMeasure 
with ClassificationMeasure {
  override def % (df: DataFrame, specimen: Specimen): MayFail[Double] = MayFail {
    import specimen._
    // NOTE: Undefined labels result in exception
    val agg = new DoubleRDDFunctions(df
      .withColumn("mpe", abs(col(outputCol) - col(labelCol)) / col(labelCol))
      .rdd.map(_.getAs[Double]("mpe")))
    agg.mean
  }

  override def isBetter(a: Double, b: Double) = a < b
}

/**
 * Calculate correlation between input and real label
 */
case object PearsonCorr extends RegressionMeasure {
  override def % (df: DataFrame, specimen: Specimen): MayFail[Double] = MayFail {
    import specimen._
    val rddX = df getDoubleRDD outputCol
    val rddY = df getDoubleRDD labelCol
    ExposedPearsonCorrelation.computeCorrelation(rddX, rddY)
  }
}

case object Precision extends ClassificationMeasure {
  override def %% (df: DataFrame, specimen: Specimen) = {
    val threshold = specimen
    pred(df, specimen).map{ rdd =>
      new BinaryClassificationMetrics(rdd)
        .precisionByThreshold
        .collectAsMap
        .toMap
    }
  }

  override def % (df: DataFrame, specimen: Specimen): MayFail[Double] = 
    Fail("Precision only returns a map of threshold -> score. Checkout %% method")
}

case object Recall extends ClassificationMeasure {
  override def %% (df: DataFrame, specimen: Specimen) = 
    pred(df, specimen).map{ rdd =>
      new BinaryClassificationMetrics(rdd)
        .recallByThreshold
        .collectAsMap
        .toMap
    }

  override def % (df: DataFrame, specimen: Specimen): MayFail[Double] = 
    Fail("Recall only returns a map of threshold -> score. Checkout %% method")
}

case object FMeasure extends ClassificationMeasure {
  override def %% (df: DataFrame, specimen: Specimen) = 
    pred(df, specimen).map{ rdd =>
      new BinaryClassificationMetrics(rdd)
        .fMeasureByThreshold
        .collectAsMap
        .toMap
    }

  override def % (df: DataFrame, specimen: Specimen): MayFail[Double] = 
    Fail("F-Measure only returns a map of threshold -> score. Checkout %% method")
}

/**
 * Area under ROC curve
 */
case object AUC extends ClassificationMeasure {
  override def % (df: DataFrame, specimen: Specimen): MayFail[Double] = {
    pred(df, specimen).map{ rdd =>
      new BinaryClassificationMetrics(rdd).areaUnderROC
    }
  }
}

/**
 * Area under Precision-recall curve
 */
case object AUCPrecisionRecall extends ClassificationMeasure {
  override def % (df: DataFrame, specimen: Specimen): MayFail[Double] = {
    pred(df, specimen).map{ rdd =>
      new BinaryClassificationMetrics(rdd).areaUnderPR
    }
  }
}


