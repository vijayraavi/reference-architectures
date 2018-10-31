package com.microsoft.pnp

import org.apache.spark.sql.SparkSession

object SparkHelper {

  var session: SparkSession = null

  def intializeSpark(cassandraHost: String, cassandraUserName: String, cassandraPassword: String) = {

    session = SparkSession
      .builder()
      .config("spark.cassandra.connection.host", cassandraHost)
      .config("spark.cassandra.connection.port", "10350")
      .config("spark.cassandra.connection.ssl.enabled", "true")
      .config("spark.cassandra.auth.username", cassandraUserName)
      .config("spark.cassandra.auth.password", cassandraPassword)
      //      .config("spark.cassandra.connection.factory", "com.microsoft.azure.cosmosdb.cassandra.CosmosDbConnectionFactory")
      .config("spark.master", "local[10]")
      .config("spark.cassandra.output.batch.size.rows", "1")
      .config("spark.cassandra.connection.connections_per_executor_max", "2")
      .config("spark.cassandra.output.concurrent.writes", "5")
      .config("spark.cassandra.output.batch.grouping.buffer.size", "300")
      .config("spark.cassandra.connection.keep_alive_ms", "5000")
      .getOrCreate()

    session


  }

  def getSparkSession() = {

    session
  }

}
