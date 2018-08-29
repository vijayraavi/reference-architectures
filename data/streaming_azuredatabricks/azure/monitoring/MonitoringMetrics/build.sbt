name := "StreamingMetrics"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.3.1"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.3.1" % "provided"
libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.3.1"

// https://mvnrepository.com/artifact/com.github.ptv-logistics/log4jala
libraryDependencies += "com.github.ptv-logistics" % "log4jala" % "1.0.4"
// https://mvnrepository.com/artifact/com.google.code.gson/gson
libraryDependencies += "com.google.code.gson" % "gson" % "2.8.5"

libraryDependencies += "org.json4s" %% "json4s-native" % "3.6.0-M4"