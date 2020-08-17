package com.tao.thundercats.samples

import com.tao.thundercats.samples.base._
import com.tao.thundercats.physical._
import com.tao.thundercats.functional.MayFail

/**
 * Main entry point
 */

object Main extends SparkBase {

  override val name = "samples"

  Console.println("Starting samples!")
  val pipeInput = for {
    // Columns : Region,Country,State,City,Month,Day,Year,AvgTemperature
    cityTemp <- Read.csv(Data.pathCityTempCSV)
    noise <- Fake.genNoise
  } yield cityTemp




  Console.println("Shutting down kernel!")
  end()

  Console.println("Ending")
}

object Fake {
  def genNoise() = MayFail {
    // TAOTODO
    ???
  }
}