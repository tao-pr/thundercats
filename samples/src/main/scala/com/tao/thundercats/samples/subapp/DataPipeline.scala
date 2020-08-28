package com.tao.thundercats.samples.subapp

import com.tao.thundercats.samples.base._
import com.tao.thundercats.physical._
import com.tao.thundercats.functional.MayFail
import com.tao.thundercats.preprocess.Text

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object DataPipeline extends BaseApp {

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
      _        <- Screen.showDF(cityTemp, Some("cityTemp (CSV)"))
      _        <- Screen.showDF(cnt, Some("Countries (CSV)"))
      agg      <- aggregate(cityTemp, cnt)
      _        <- Screen.showDF(agg, Some("Aggregate"))
    } yield agg

    if (pipeInput.isFailing){
      Console.println("[ERROR] reading inputs")
      Console.println(pipeInput.getError)
    }

    // STEP #2 : Aggregate data, write to parquets
    // ----------------------------------------
    val pipeOutput = for {
      cityTemp   <- pipeInput
      timeSeries <- Transform.apply(cityTemp, df => {
        val w = Window.partitionBy(col("Country"), col("Year")).orderBy("Month")
        val monthly = cityTemp
          .groupBy("Country", "Month", "Year")
          .agg(avg("AvgTemperature").alias("AvgTemperature"))
          .groupBy("Country", "Month")
          .agg(
            collect_list("AvgTemperature").alias("MontlyTemperatures"),
            min("AvgTemperature").alias("MinTemperature"),
            max("AvgTemperature").alias("MaxTemperature"),
            avg("AvgTemperature").alias("MeanTemperature"))

        monthly
      })
      _ <- Screen.showDF(timeSeries, Some("Output to be written to parquet"))
      _ <- Write.parquet(timeSeries, 
        Data.pathOutputParquet(System.getProperty("user.home")), 
        Write.NoPartition,
        overwrite = true)
    } yield timeSeries

    if (pipeOutput.isFailing){
      Console.println("[ERROR] writing outputs")
      Console.println(pipeOutput.getError)
    }
    else {
      Console.println("Output written to parquet!")
    }

    // STEP #3 : Read parquet back as we wrote
    // ---------------------------------------
    // TAOTODO

  }

  private def aggregate(cityTemp: DataFrame, countries: DataFrame): MayFail[DataFrame] = 
    for {
      temp <- Filter.where(cityTemp, col("year") >= 2000)
      temp <- Text.trim(temp, "Country")
      cnt  <- Text.trim(countries, "Country")
      j <- Join.inner(temp, cnt, Join.On("Country" :: Nil))
      _ <- Screen.showSchema(j)
    } yield j

}

