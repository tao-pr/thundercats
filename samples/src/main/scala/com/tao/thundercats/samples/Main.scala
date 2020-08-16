package com.tao.thundercats.samples

import com.tao.thundercats.samples.base._
import com.tao.thundercats.physical._

/**
 * Main entry point
 */

object Main extends App {

  Console.println("HELLO!")
  Console.println("Choose what to run:")
  Console.println(" (1) - Physical file pipe sample")
  Console.println(" (0) - Exit")

  val choice = scala.io.StdIn.readInt()

  choice match {
    case 0 => Console.println("BYE!")
    case 1 => PhysicalFilePipe.run
  }


}