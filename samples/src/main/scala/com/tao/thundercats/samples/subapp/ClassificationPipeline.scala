package com.tao.thundercats.samples.subapp

import com.tao.thundercats.samples.base._
import com.tao.thundercats.physical._
import com.tao.thundercats.functional.MayFail
import com.tao.thundercats.preprocess.Text
import com.tao.thundercats.model._
import com.tao.thundercats.estimator._
import com.tao.thundercats.evaluation._

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
      cnt      <- Transform.select(cnt, Seq("Country", "Region"))
      _        <- Screen.showDF(cityTemp, Some("cityTemp (CSV)"), Show.HideComplex)
      _        <- Screen.showDF(cnt, Some("Countries (CSV)"), Show.HideComplex)
      clean    <- cleanAgg(cityTemp, cnt)
    } yield clean
    
    val pipeInspect = for { 
      clean <- pipeInput 
      _     <- Screen.showDF(clean, Some("Clean training set"), Show.HideComplex)
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
      g  <- Group.agg(
        clean,
        by=col("isTempRising")::Nil,
        agg=Group.Agg(
          count("month").alias("numRecords") :: Nil
        ))

      _  <- Screen.showDF(t, Some("Brief stats"), Show.Max(10))
      _  <- Screen.showDF(g, Some("Target stats"), Show.Max(10))
      
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


    if (pipeInput.isFailing){
      Console.println("[ERROR] reading inputs")
      Console.println(pipeInput.getError)
    }

    if (pipeInspect.isFailing){
      Console.println("[ERROR] inspecting data")
      Console.println(pipeInspect.getError)
    }
    else {
      // Modeling
      val model = trainClassifier(pipeInspect.get)
    }
  }

  private def cleanAgg(cityTemp: DataFrame, countries: DataFrame): MayFail[DataFrame] = 
    for {
      temp <- Filter.where(cityTemp, col("year") >= 2000)
      temp <- MayFail{ temp.withColumn("AvgTemperature", (col("AvgTemperature") - 32.0) * 0.5556) }
      temp <- Text.trim(temp, "Country")
      cnt  <- Text.trim(countries, "Country")
      joined <- Join.inner(temp, cnt, Join.On("Country" :: Nil))
      group  <- Group.agg(
        joined, by=col("Country")::col("Year")::col("Month")::Nil, 
        agg=Group.Agg(
          mean("AvgTemperature").alias("AvgTemperature") :: 
          first("Region").alias("Region") ::
          Nil))
      timeSeries <- Transform.apply( group, joined => {
        val wnd = Window.partitionBy(col("Country")).orderBy("year", "month")
        joined
          .withColumn("prevTemperature", lag("AvgTemperature",1).over(wnd))
          .filter(col("prevTemperature").isNotNull)
          .withColumn("isTempRising", col("AvgTemperature") > col("prevTemperature"))
      })
      _ <- Screen.showSchema(timeSeries)
    } yield timeSeries

  private def trainClassifier(data: DataFrame): Specimen = {
    // Classify if the temperature is gonna go up or not
    val features = Seq("Year", "Month", "AvgTemperature", "Region_encoded")
    val encoder = Features.encodeStrings(
      data, Murmur, suffix="_encoded"
    )
    val estimator = Preset.decisionTree(
      features=AssemblyFeature(features, "features"),
      labelCol="isTempRising",
      outputCol="predictedRising")
    val design = FeatureModelDesign(
      outputCol="predictedRising",
      labelCol="isTempRising",
      estimator=estimator,
      featurePipe=Some(encoder))

    Console.println("Training decision tree ...")

    design.toSpecimen(AssemblyFeature(features, "features"), data)
  }
}