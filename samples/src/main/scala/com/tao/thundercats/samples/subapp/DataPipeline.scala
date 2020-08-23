package com.tao.thundercats.samples.subapp

import com.tao.thundercats.samples.base._
import com.tao.thundercats.physical._
import com.tao.thundercats.functional.MayFail

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Dataset, DataFrame}

object DataPipeline extends BaseApp {

  override def runMe(implicit spark: SparkSession) = {
    Console.println("Reading input sources ...")
    val pipeInput = for {
      // Columns : Region,Country,State,City,Month,Day,Year,AvgTemperature
      cityTemp <- Read.csv(Data.pathCityTempCSV(System.getProperty("user.home")))
      cnt      <- Read.csv(Data.pathCountryCSV(System.getProperty("user.home")))
      cityTemp <- Read.select(cityTemp, Seq("Country", "Month", "Day", "Year", "AvgTemperature"))
      cnt      <- Read.select(cnt, Seq("Country", "Population", "Area", "PopDensity"))
      _        <- Screen.showDF(cityTemp, Some("cityTemp (CSV)"))
      _        <- Screen.showDF(cnt, Some("Countries (CSV)"))
    } yield cityTemp

    if (pipeInput.isFailing){
      Console.println("[ERROR] reading inputs")
      Console.println(pipeInput.getError)
    }

  }

  private def aggregate(cityTemp: DataFrame): MayFail[DataFrame] = MayFail {
    ???
  }

}

