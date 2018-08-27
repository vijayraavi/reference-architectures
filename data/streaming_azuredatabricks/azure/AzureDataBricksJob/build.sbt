name := "AzureDataBricksJob"

version := "0.1"

scalaVersion := "2.11.8"

val scalaTest = "org.scalatest" %% "scalatest" % "3.0.5"
val gigahorse = "com.eed3si9n" %% "gigahorse-okhttp" % "0.3.1"
val playJson = "com.typesafe.play" %% "play-json" % "2.6.9"

lazy val AzureDataBricksJob = (project in file("."))
  .settings(
    name := "AzureDataBricksJob",
    libraryDependencies ++= Seq(gigahorse, playJson),
    libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.3.1",
    libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.3.1" % "provided",
    libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.3.1",
    libraryDependencies += "com.microsoft.azure" % "azure-eventhubs-spark_2.11" % "2.3.1",
    libraryDependencies += "org.geotools" % "gt-shapefile" % "19.2" from "http://download.osgeo.org/webdav/geotools/",
    libraryDependencies += scalaTest % Test,
    dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.8.9",
    dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.9",
    dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.8.9",
  )