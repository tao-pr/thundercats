package com.tao.thundercats.samples.subapp

import com.tao.thundercats.samples.base._
import com.tao.thundercats.physical._
import com.tao.thundercats.functional.MayFail
import com.tao.thundercats.preprocess.Text

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.functions._

object DataPipeline extends BaseApp {

  override def runMe(implicit spark: SparkSession) = {
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
    } yield cityTemp

    if (pipeInput.isFailing){
      Console.println("[ERROR] reading inputs")
      Console.println(pipeInput.getError)
    }

    // Cook the output
    val pipeOutput = for {
      cityTemp   <- pipeInput
      timeSeries <- Transform.apply(cityTemp, df => {
        df // TAOTODO
      })
    } yield timeSeries

    if (pipeOutput.isFailing){
      Console.println("[ERROR] writing outputs")
      Console.println(pipeOutput.getError)
    }

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

