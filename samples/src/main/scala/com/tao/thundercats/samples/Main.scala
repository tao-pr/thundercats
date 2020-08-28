package com.tao.thundercats.samples

import com.tao.thundercats.samples.base._
import com.tao.thundercats.samples.subapp._
import com.tao.thundercats.physical._
import com.tao.thundercats.functional.MayFail


/**
 * Main entry point
 */

object Main extends SparkBase {

  override val name = "samples"
  val mode = "data-pipeline"

  Console.println("Starting samples!")
  val app: BaseApp = mode match {
    case "data-pipeline" => DataPipeline
  }

  Console.println(s"Running : ${app.getClass.getName}")
  app.runMe(spark)

  Console.println("Shutting down kernel!")
  end()
}
