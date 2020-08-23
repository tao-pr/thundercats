package com.tao.thundercats.samples.base

object Data {

  def pathCityTempCSV(rootPath: String) = getPath("/city_temperature.csv", rootPath)
  def pathCountryCSV(rootPath: String) = getPath("/countries-of-the-world.csv", rootPath)

  private def getPath(file: String, rootPath: String = "") = 
    if (rootPath.isEmpty)
      getClass.getResource(file).getPath
    else
      s"${rootPath}/data/${file}"

}