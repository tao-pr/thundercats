package com.tao.thundercats.samples.subapp

trait BaseApp {
  def runMe(args: Seq[String] = Nil): Unit
}