package com.tao.thundercats.samples

import com.tao.thundercats.samples.base._
import com.tao.thundercats.physical._

/**
 * Main entry point
 */

object PhysicalFilePipe extends SparkBase {

  override val name = "PhysicalFilePipe-Sample"

  def run(){

    Console.println(Console.GREEN + 
      s"Reading sample csv : ${Data.pathCityTempCSV}" +
      Console.RESET)

    // Define data pipeline
    val pipeInput = Read.csv(Data.pathSampleCSV)


  }

}