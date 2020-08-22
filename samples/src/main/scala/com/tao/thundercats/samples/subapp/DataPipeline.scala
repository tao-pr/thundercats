package com.tao.thundercats.samples.subapp

import com.tao.thundercats.samples.base._
import com.tao.thundercats.physical._
import com.tao.thundercats.functional.MayFail

import org.apache.spark.sql.SparkSession

object DataPipeline extends BaseApp {

  override def runMe(implicit spark: SparkSession) = {
    val pipeInput = for {
      // Columns : Region,Country,State,City,Month,Day,Year,AvgTemperature
      cityTemp <- Read.csv(Data.pathCityTempCSV)
      _ <- Screen.showDF(cityTemp, Some("cityTemp (CSV)"))
    } yield cityTemp



  }

}

