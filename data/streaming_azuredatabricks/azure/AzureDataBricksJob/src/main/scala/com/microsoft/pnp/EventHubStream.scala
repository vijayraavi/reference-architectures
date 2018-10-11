package com.microsoft.pnp

import org.apache.spark.sql.SparkSession
import org.apache.spark.eventhubs._

object EventHubStream {

  def BuildConnectionString(connectionString: String, eventHubName: String): String = {
    ConnectionStringBuilder(connectionString)
      .setEventHubName(eventHubName)
      .build
  }

  def ExecuteDataFrame(spark: SparkSession, connectionString: String): org.apache.spark.sql.DataFrame = {
    val eventHubConf = EventHubsConf(connectionString)
      .setStartingPosition(EventPosition.fromStartOfStream)

    spark.readStream
      .format(SparkStreamFormat)
      .options(eventHubConf.toMap)
      .load
  }
}
