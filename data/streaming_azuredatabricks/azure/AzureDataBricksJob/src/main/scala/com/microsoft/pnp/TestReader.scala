package com.microsoft.pnp


import java.util.{Base64, UUID}

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.SparkConf
import org.apache.spark.eventhubs.{ConnectionStringBuilder, EventHubsConf, EventPosition}
import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}

case class MockObject(field1: String, field2: String)

class CassandraSinkForeach() extends ForeachWriter[org.apache.spark.sql.Row] {
  // This class implements the interface ForeachWriter, which has methods that get called
  // whenever there is a sequence of rows generated as output

  var cassandraDriver: CassandraDriver = null;

  def open(partitionId: Long, version: Long): Boolean = {
    // open connection
    println(s"Open connection")
    true
  }

  def process(record: org.apache.spark.sql.Row) = {
    println(s"Process new $record")
    if (cassandraDriver == null) {
      cassandraDriver = new CassandraDriver();
    }
    cassandraDriver.connector.withSessionDo(session =>
      session.execute(
        s"""
       insert into ${cassandraDriver.namespace}.${cassandraDriver.foreachTableSink} (field1, field2)
       values('${record(0)}', '${record(1)}')""")
    )
  }

  def close(errorOrNull: Throwable): Unit = {
    // close the connection
    println(s"Close connection")
  }
}

class CassandraDriver extends SparkSessionBuilder {
  // This object will be used in CassandraSinkForeach to connect to Cassandra DB from an executor.
  // It extends SparkSessionBuilder so to use the same SparkSession on each node.
  val spark = buildSparkSession

  import spark.implicits._

  val connector = CassandraConnector(spark.sparkContext.getConf)

  val namespace = "nsp"
  val foreachTableSink = "test"
}

class SparkSessionBuilder extends Serializable {
  // Build a spark session. Class is made serializable so to get access to SparkSession in a driver and executors.
  // Note here the usage of @transient lazy val
  def buildSparkSession: SparkSession = {
    @transient lazy val conf: SparkConf = new SparkConf()
      .setAppName("Structured Streaming from eventhub to Cassandra")
      .set("spark.cassandra.connection.host", "nithintest.cassandra.cosmosdb.azure.com")
      .set("spark.cassandra.connection.port", "10350")
      //      .set("spark.cassandra.connection.ssl.enabled", "true")
      .set("spark.cassandra.auth.username", "nithintest")
      .set("spark.cassandra.auth.password", "AZA3VkgVQ6ONz9xBCEFKqU8Mp4ZxBljz1GbIuFYJWRgcGOipLszDT56nK0xJSaVN7ozP1ZelDd6PIixmREFTbQ==")
      .set("spark.sql.streaming.checkpointLocation", "checkpoint")
      .set("spark.cassandra.connection.factory", "com.microsoft.azure.cosmosdb.cassandra.CosmosDbConnectionFactory")
      .set("spark.master", "local[10]")
      .set("spark.cassandra.output.batch.size.rows", "1") //Leave this as one for Cosmos DB
      .set("spark.cassandra.connection.connections_per_executor_max", "2") //Maximum number of connections per Host set on each Executor JVM - default parallelism/executors for Spark Commands
      .set("spark.cassandra.output.concurrent.writes", "5") //Maximum number of batches executed in parallel by a single Spark task
      .set("spark.cassandra.output.batch.grouping.buffer.size", "300") //How many batches per single Spark task can be stored in memory before sending to Cassandra
      .set("spark.cassandra.connection.keep_alive_ms", "5000") //Period of time to keep unused connections open

    @transient lazy val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    spark
  }
}

object TestReader extends SparkSessionBuilder {

  def main(args: Array[String]) {

    val spark = buildSparkSession

//    val spark = SparkSession.builder().config("spark.master", "local[10]").getOrCreate()


    import spark.implicits._

    val secret = "Endpoint=sb://rs-eh-ns.servicebus.windows.net/;SharedAccessKeyName=taxi-ride-asa-access-policy;SharedAccessKey=/GjrSZc1uXKlwnrbXikYUEwcC++zE9nGJm4cRmvUlvw=;EntityPath=taxi-ride"
    val rideConnectionString = ConnectionStringBuilder(secret)
      .setEventHubName("taxi-ride")
      .build

    val rideEventHubConf = EventHubsConf(rideConnectionString)
      .setStartingPosition(EventPosition.fromStartOfStream)

    val rideDataFrame = spark.readStream
      .format("eventhubs")
      .options(rideEventHubConf.toMap)
      .load

    val rides = rideDataFrame
      .selectExpr("cast(body as string) AS rideContent",
        "enqueuedTime As recordIngestedTime")

    val mockObjects = rides.map(row => Base64Converter(row)).toDF()


    mockObjects
      .writeStream
      .outputMode("update")
//      .format("console")
      .foreach(new CassandraSinkForeach())
      .start()
      .awaitTermination()

    spark.stop
  }

  def Base64Converter(row: Row): MockObject = {

    val field1 = new String(Base64.getEncoder.encodeToString(row(0).toString.getBytes()))
    val field2 = UUID.randomUUID.toString

    MockObject(
      field1,
      field2
    )


  }

}
