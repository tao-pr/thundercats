package com.tao.thundercats.samples.subapp

import com.tao.thundercats.samples.base._
import com.tao.thundercats.physical._
import com.tao.thundercats.functional.MayFail
import com.tao.thundercats.preprocess.Text

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object ClassificationPipeline extends BaseApp {

  override def runMe(implicit spark: SparkSession) = {

    // STEP #1 : Read CSVs 
    // ----------------------------------------
    Console.println("Reading input sources ...")
    val pipeInput = for {
      // Columns : Region,Country,State,City,Month,Day,Year,AvgTemperature
      cityTemp <- Read.csv(Data.pathCityTempCSV(System.getProperty("user.home")))
      cnt      <- Read.csv(Data.pathCountryCSV(System.getProperty("user.home")))
      cityTemp <- Transform.select(cityTemp, Seq("Country", "Month", "Day", "Year", "AvgTemperature"))
      cnt      <- Transform.select(cnt, Seq("Country", "Population", "Area", "PopDensity"))
      _        <- Screen.showDF(cityTemp, Some("cityTemp (CSV)"), Show.HideComplex)
      _        <- Screen.showDF(cnt, Some("Countries (CSV)"), Show.HideComplex)
      clean    <- cleanAgg(cityTemp, cnt)
    } yield clean
    
    val pipeInspect = for { 
      clean <- pipeInput 
      _     <- Screen.showDF(clean, Some("Clean data (aggregate)"), Show.HideComplex)
      t     <- Group.agg(
        clean, 
        by=col("year")::Nil, 
        agg=Group.Agg(
          collect_set("month").alias("months") ::
          approx_count_distinct("Country").alias("numCountries") ::
          min("AvgTemperature").alias("minTemp") ::
          max("AvgTemperature").alias("maxTemp") :: Nil
        ))
      t  <- Order.by(t, "year" :: Nil)
      _  <- Screen.showDF(t, Some("Brief stats"), Show.All)
      
      // Inspect subset
      th <- Filter.where(clean, 
        (col("Country").isInCollection("Spain"::"Germany"::"Thailand"::"Russia"::Nil)) && 
        (col("year")===2010))
      th <- Group.agg(th, col("Country")::Nil, Group.Agg(
          min("AvgTemperature").alias("minTemp") ::
          max("AvgTemperature").alias("maxTemp") ::
          mean("AvgTemperature").alias("meanTemp") :: Nil
        ))
      _  <- Screen.showDF(th, Some("Subset inspection"), Show.Max(10))
    } yield clean

  }

  private def cleanAgg(cityTemp: DataFrame, countries: DataFrame): MayFail[DataFrame] = 
    for {
      temp <- Filter.where(cityTemp, col("year") >= 2000)
      temp <- MayFail{ temp.withColumn("AvgTemperature", (col("AvgTemperature") - 32.0) * 0.5556) }
      //temp <- Filter.where(temp, abs(col("AvgTemperature")) < 40)
      temp <- Text.trim(temp, "Country")
      cnt  <- Text.trim(countries, "Country")
      j <- Join.inner(temp, cnt, Join.On("Country" :: Nil))
      _ <- Screen.showSchema(j)
    } yield j
}