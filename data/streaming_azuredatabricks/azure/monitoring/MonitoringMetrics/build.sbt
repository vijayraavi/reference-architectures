name := "StreamingMetrics"

version := "0.1"

scalaVersion := "2.11.8"


// https://mvnrepository.com/artifact/org.scala-lang/scala-library
libraryDependencies += "org.scala-lang" % "scala-library" % "2.11.8"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.3.1"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.3.1" % "provided"
libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.3.1"
// https://mvnrepository.com/artifact/io.dropwizard.metrics/metrics-core
libraryDependencies += "io.dropwizard.metrics" % "metrics-core" % "3.1.5"
// https://mvnrepository.com/artifact/org.eclipse.jetty/jetty-server
libraryDependencies += "org.eclipse.jetty" % "jetty-server" % "9.3.20.v20170531"


libraryDependencies += "com.microsoft.azure" % "azure-eventhubs-spark_2.11" % "2.3.2"

// https://mvnrepository.com/artifact/com.github.ptv-logistics/log4jala
libraryDependencies += "com.github.ptv-logistics" % "log4jala" % "1.0.4"
// https://mvnrepository.com/artifact/com.google.code.gson/gson
libraryDependencies += "com.google.code.gson" % "gson" % "2.8.5"

libraryDependencies += "org.json4s" %% "json4s-native" % "3.6.0-M4"

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.8.9"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.9"
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.8.9"


assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}