package com.microsoft.pnp

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object SparkHelper {

  def getAndConfigureSparkSession() = {


    val sc = SparkContext.getOrCreate().getConf
      .setAppName("Structured Streaming from eventhub to Cassandra")
      .set("spark.cassandra.connection.host", "nithintest.cassandra.cosmosdb.azure.com")
      .set("spark.cassandra.connection.port", "10350")
      .set("spark.cassandra.connection.ssl.enabled", "true")
      .set("spark.cassandra.auth.username", "nithintest")
      .set("spark.cassandra.auth.password", "AZA3VkgVQ6ONz9xBCEFKqU8Mp4ZxBljz1GbIuFYJWRgcGOipLszDT56nK0xJSaVN7ozP1ZelDd6PIixmREFTbQ==")
      .set("spark.sql.streaming.checkpointLocation", "checkpoint")
      .set("spark.cassandra.connection.factory", "com.microsoft.azure.cosmosdb.cassandra.CosmosDbConnectionFactory")
      .set("spark.master", "local[10]")
      .set("spark.cassandra.output.batch.size.rows", "1") //Leave this as one for Cosmos DB
      .set("spark.cassandra.connection.connections_per_executor_max", "2") //Maximum number of connections per Host set on each Executor JVM - default parallelism/executors for Spark Commands
      .set("spark.cassandra.output.concurrent.writes", "5") //Maximum number of batches executed in parallel by a single Spark task
      .set("spark.cassandra.output.batch.grouping.buffer.size", "300") //How many batches per single Spark task can be stored in memory before sending to Cassandra
      .set("spark.cassandra.connection.keep_alive_ms", "5000")


    val session = SparkSession
      .builder()
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
