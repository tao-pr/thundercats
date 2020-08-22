package com.tao.thundercats.samples.subapp

import com.tao.thundercats.samples.base._
import com.tao.thundercats.physical._
import com.tao.thundercats.functional.MayFail

object DataPipeline extends BaseApp {

  override def runMe(args: Seq[String]) = {
    val pipeInput = for {
      // Columns : Region,Country,State,City,Month,Day,Year,AvgTemperature
      cityTemp <- Read.csv(Data.pathCityTempCSV)
      noise <- Fake.genNoise
    } yield cityTemp

    



  }

}

