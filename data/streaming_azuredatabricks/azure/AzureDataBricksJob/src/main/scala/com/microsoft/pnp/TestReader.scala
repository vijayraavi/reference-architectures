package com.microsoft.pnp

import java.util.{Base64, UUID}

import org.apache.spark.eventhubs.{ConnectionStringBuilder, EventHubsConf, EventPosition}
import org.apache.spark.sql.Row

case class MockObject(field1: String, field2: String)

object TestReader {

  def main(args: Array[String]) {


    val cassandraEndPoint = "nithintest.cassandra.cosmosdb.azure.com"
    val cassandraUserName = "nithintest"
    val cassandraPassword = "AZA3VkgVQ6ONz9xBCEFKqU8Mp4ZxBljz1GbIuFYJWRgcGOipLszDT56nK0xJSaVN7ozP1ZelDd6PIixmREFTbQ=="
    val spark = SparkHelper
      .intializeSpark(cassandraEndPoint,
        cassandraUserName,
        cassandraPassword)

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

    rides.printSchema()
    val mockObjects = rides.map(row => Base64Converter(row)).toDF()


    TestCassandraDriver.saveForeach(mockObjects)

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


  import com.datastax.spark.connector.cql.CassandraConnector
  import org.apache.spark.sql.DataFrame

  object TestCassandraDriver {

    val spark = SparkHelper.getSparkSession()

    import spark.implicits._

    val connector = CassandraConnector(SparkHelper.getSparkSession().sparkContext.getConf)

    val namespace = "nsp"

    val foreachTableSink = "test"

    def saveForeach(df: DataFrame) = {

      df.as[MockObject]
        .writeStream
        .queryName("eventHubtoCassandraInsert")
        .outputMode("update")
        .foreach(new TestCassandraSinkForeach())
        .start()
        .awaitTermination()

    }
  }
}

import com.microsoft.pnp.TestReader.TestCassandraDriver
import org.apache.spark.sql.ForeachWriter

class TestCassandraSinkForeach() extends ForeachWriter[MockObject] {

  private def getInsertStatement(mockObject: MockObject): String =
    s"""
       |insert into ${TestCassandraDriver.namespace}.${TestCassandraDriver.foreachTableSink} (field1, field2)
       |       values('${mockObject.field1}', '${mockObject.field2}')"""


  override def open(partitionId: Long, version: Long): Boolean = {

    true
  }

  override def process(mockObject: MockObject): Unit = {

    TestCassandraDriver.connector.withSessionDo(session => session.execute(getInsertStatement(mockObject)))

  }

  override def close(errorOrNull: Throwable): Unit = {

  }
}
