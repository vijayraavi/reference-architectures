package com.microsoft.pnp

object NeighborhoodEnvReader {


  def getNeighborhoodFilePath(): String = {

    var neighbourhoodFilePath = System.getenv("NEIGHBOURHOOD_FILE_PATH")
    if ((neighbourhoodFilePath == null) || (neighbourhoodFilePath.trim == "")) {

      // this is hook for unit test cases
      neighbourhoodFilePath = "./ZillowNeighborhoods-NY.zip"
    }

    neighbourhoodFilePath
  }
}