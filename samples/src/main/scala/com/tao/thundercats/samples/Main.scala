package com.tao.thundercats.samples

import com.tao.thundercats.samples.base._
import com.tao.thundercats.physical._

/**
 * Main entry point
 */

object Main extends SparkBase {

  override val name = "samples"

  Console.println("HELLO!")
  val pipeInput = Read.csv(Data.pathCityTempCSV)



  Console.println("SHUTTING DOWN!")
  stop()
}