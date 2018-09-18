package com.microsoft.pnp

import java.net.URL

object NeighborhoodEnvReader {


  def getNeighborhoodFileURL(): URL = {

    val neighborhoodFilePath = System.getenv("NEIGHBORHOOD_FILE_PATH")
    if ((neighborhoodFilePath == null) || (neighborhoodFilePath.trim == "")) {
      // this is hook for unit test cases
      new URL(
        String.format(
          "jar:%s!/ZillowNeighborhoods-NY.shp",
          getClass.getClassLoader.getResource("./ZillowNeighborhoods-NY.zip")
        )
      )
    } else {
      new URL(neighborhoodFilePath)
    }
  }
}