package com.microsoft.pnp


import org.apache.spark.eventhubs.{ConnectionStringBuilder, EventHubsConf, EventPosition}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object TaxiCabReader {

  val taxiRideEventHubName = "taxi-ride"
  val taxiFareEventHubName = "taxi-fare"
  val sparkReadStreamFormatForEventHub = "eventhubs"

  def main(args: Array[String]) {

    val spark = SparkSession.builder().config("spark.master", "local").getOrCreate()
    import spark.implicits._

    // Taxi ride infra setup -
    val rideEventHubSharedAccessKey = ""

    val rideConnectionString = ConnectionStringBuilder(rideEventHubSharedAccessKey)
      .setEventHubName(taxiRideEventHubName)
      .build

    val rideEventHubConf = EventHubsConf(rideConnectionString)
      .setStartingPosition(EventPosition.fromStartOfStream)

    //Taxi fare infra setup -
    val fareEventHubSharedAccessKey = ""

    val fareConnectionString = ConnectionStringBuilder(fareEventHubSharedAccessKey)
      .setEventHubName(taxiFareEventHubName)
      .build

    val fareEventHubConf = EventHubsConf(fareConnectionString)
      .setStartingPosition(EventPosition.fromStartOfStream)


    //Taxi ride read stream and extract taxi ride object -
    val rideDataFrame = spark.readStream
      .format(sparkReadStreamFormatForEventHub)
      .options(rideEventHubConf.toMap)
      .load


    val rides = rideDataFrame
      .selectExpr("cast(body as string) AS rideContent",
        "enqueuedTime As recordIngestedTime")


    val taxiRideStream = rides.map(eventHubRow => TaxiRideMapper.mapRowToEncrichedTaxiRideRecord(eventHubRow))
      .withWatermark("rideEnqueueTime", "15 minutes")
      .filter(rideRecord => rideRecord.isValidRideRecord)
      .as[EnrichedTaxiRideRecord]



    //TODO - invalid rides should be sent to invalid ride monitoring sinks

    // Taxi fare read stream and extract fare object -
    val fareDataFrame = spark.readStream
      .format("eventhubs")
      .options(fareEventHubConf.toMap)
      .load

    val fares = fareDataFrame
      .selectExpr("cast (body as string) AS fareContent",
        "enqueuedTime As recordIngestedTime")


    val taxiFareStream = fares.map(eventHubRow => TaxiFareMapper.mapRowToEncrichedTaxiFareRecord(eventHubRow))
      .withWatermark("fareEnqueueTime", "15 minutes")
      .filter(fareRecord => fareRecord.isValidFareRecord)
      .as[EnrichedTaxiFareRecord]



    // TODO - invalid fare should be sent to invalid fare monitoring sinks

    // merge two streams based on a key .
    // assumption fare could reach later than ride in this case .
    // max threshold would be 15 mins
    val mergedStream = taxiFareStream.join(taxiRideStream, expr(
      """
      rideRecordKey = fareRecordKey AND
      fareEnqueueTime >= rideEnqueueTime AND
      fareEnqueueTime <= rideEnqueueTime + interval 15 minutes
      """
    ),
      "leftOuter"
    )


    mergedStream.writeStream.outputMode("append").format("console").option("truncate", value = false).start().awaitTermination()


    spark.stop()
  }


}
