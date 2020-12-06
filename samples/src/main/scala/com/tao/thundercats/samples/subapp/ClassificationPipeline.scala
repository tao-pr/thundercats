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

    // Override Log level (if needed)
    //spark.sparkContext.setLogLevel("INFO")

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
      Log.info("Evaluating & Training classifier ...")
      val model = trainClassifier(pipeInspect.get)

      Log.info("DONE")
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
          .withColumn("isTempRising", 
            when(col("AvgTemperature") > col("prevTemperature"), 1.0).otherwise(0.0))
      })
      _ <- Screen.showSchema(timeSeries)
    } yield timeSeries

  private def trainClassifier(data: DataFrame): Specimen = {
    // Classify if the temperature is gonna go up or not
    val features = Seq("Year", "Month", "AvgTemperature", "Region_encoded")
    val encoder = Features.encodeStrings(
      data, Murmur, suffix="_encoded"
    )
    // Model design for "real training"
    val estimator = Preset.decisionTree(
      features=AssemblyFeature(features, "features"),
      labelCol="isTempRising",
      outputCol="predictedRising")
    val design = SupervisedModelDesign(
      outputCol="predictedRising",
      labelCol="isTempRising",
      estimator=estimator,
      featurePipe=Some(encoder))
    
    // Model design for "feature evaluation"
    val estimatorForEval = Preset.decisionTree(
      features=Feature("features"), // <--- use output from [FeatureCompare].allOf
      labelCol="isTempRising",
      outputCol="predictedRising")
    val designForEval = SupervisedModelDesign(
      outputCol="predictedRising",
      labelCol="isTempRising",
      estimator=estimatorForEval,
      featurePipe=Some(encoder))
    val selector = new FeatureAssemblyGenerator(
        minFeatureCombination=1,
        maxFeatureCombination=4,
        ignoreCols=Nil)
    // Mock "Region_encoded" column which does not exist here in [data]
    val combinations = selector.genCombinations(
      estimatorForEval, 
      data.withColumn("Region_encoded", lit(0)).select(features.head, features.tail:_*))

    // Only select combinations of features (length=1,3,4)
    val subCombinations = combinations.filter(_.size != 2)

    // Feature evaluation
    Console.println("Evaluating features (with MAE)")
    new ClassificationFeatureCompare(MAE)
      .allOf(designForEval, subCombinations, data)
      .foreach{ case (score,specimen) =>
        val feature = specimen.featureCol.sourceColName
        Console.println(f"... Feature : ${feature}, MAE = $score%.3f")
      }
    
    // Real training
    Console.println("Training decision tree ...")
    val Array(trainData, testData) = data.randomSplit(Array(0.8, 0.2))
    val spec = design.toSpecimen(AssemblyFeature(features, "features"), trainData)

    // Evaluate the model (precision-recall)
    val precisionScoreMap = spec.scoreMap(testData, Precision).get
    for ((k,v) <- precisionScoreMap){
      Console.println(f"... threshold : $k%.1f, score = $v%.3f")
    }

    spec
  }

}