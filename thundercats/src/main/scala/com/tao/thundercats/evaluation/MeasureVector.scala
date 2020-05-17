package com.tao.thundercats.evaluation

import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.ml.classification.LogisticRegressionModel

import breeze.linalg.DenseVector

import com.tao.thundercats.physical._
import com.tao.thundercats.functional._
import com.tao.thundercats.physical.Implicits._
import com.tao.thundercats.estimator._


/**
 * Individual measure of each feature used in the specimen
 */
trait MeasureVector extends BaseMeasure[Array[Double]] {

  /**
   * Create a score vector, each element represents each column in order
   */
  override def % (df: DataFrame, specimen: Specimen): MayFail[Array[Double]]
  def findBest(zippedScore: Array[(Double, String)]) = zippedScore.max
}

trait RegressionMeasureVector extends MeasureVector

/**
 * ZScore vector of each feature
 */
case object ZScore extends RegressionMeasureVector {
  override def % (df: DataFrame, specimen: Specimen) = MayFail {
    import specimen._

    /***
      zj            = ßj/sigma.sqrt(vj), 

      where vj      = 1/xj^2
            sigma^2 = (1/N-M-1) sum[i<-N](yi - f(xi))^2
    **/

    // Extract coefficients of logistic regression model
    val betas  = specimen.model.asInstanceOf[LogisticRegressionModel].coefficients
    val N      = df.count.toFloat
    val M      = df.columns.size.toFloat
    val sigma2 = (1/N-M-1) * df.sumOfSqrDiff(specimen.labelCol, specimen.outputCol)
    val sigma  = scala.math.sqrt(sigma2)
    val sumX2  = specimen.featureCol.asArray.map{ c =>
      df.sumOfSqr(c)
    }

    betas.toArray.zip(sumX2).map{ case (beta, sumx2) => 
      beta / (sigma * scala.math.sqrt(1/sumx2))
    }
  }

  override def findBest(zippedScore: Array[(Double, String)]) = {
    zippedScore.min
  }
}

/**
 * Significance vector of each feature
 */
case class Significance(level: Double = 0.95) extends RegressionMeasureVector {
  override def % (df: DataFrame, specimen: Specimen) = MayFail {
    import specimen._

    ???
  }

  override def findBest(zippedScore: Array[(Double, String)]) = {
    zippedScore.max
  }
}