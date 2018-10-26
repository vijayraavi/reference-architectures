package com.microsoft.pnp

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object SparkHelper {

  def getAndConfigureSparkSession() = {


    val session = SparkSession
      .builder()
      .config("spark.cassandra.connection.host", "nithintest.cassandra.cosmosdb.azure.com")
      .config("spark.cassandra.connection.port", "10350")
      .config("spark.cassandra.connection.ssl.enabled", "true")
      .config("spark.cassandra.auth.username", "nithintest")
      .config("spark.cassandra.auth.password", "AZA3VkgVQ6ONz9xBCEFKqU8Mp4ZxBljz1GbIuFYJWRgcGOipLszDT56nK0xJSaVN7ozP1ZelDd6PIixmREFTbQ==")
      .config("spark.sql.streaming.checkpointLocation", "checkpoint")
      .config("spark.cassandra.connection.factory", "com.microsoft.azure.cosmosdb.cassandra.CosmosDbConnectionFactory")
      .config("spark.master", "local[10]")
      .config("spark.cassandra.output.batch.size.rows", "1") //Leave this as one for Cosmos DB
      .config("spark.cassandra.connection.connections_per_executor_max", "2") //Maximum number of connections per Host set on each Executor JVM - default parallelism/executors for Spark Commands
      .config("spark.cassandra.output.concurrent.writes", "5") //Maximum number of batches executed in parallel by a single Spark task
      .config("spark.cassandra.output.batch.grouping.buffer.size", "300") //How many batches per single Spark task can be stored in memory before sending to Cassandra
      .config("spark.cassandra.connection.keep_alive_ms", "5000")
      .getOrCreate()

    SparkContext.getOrCreate().getConf.getAll.foreach(x => println(x))

    session

  }

  def getSparkSession() = {

    SparkSession
      .builder()
      .getOrCreate()

  }
}
