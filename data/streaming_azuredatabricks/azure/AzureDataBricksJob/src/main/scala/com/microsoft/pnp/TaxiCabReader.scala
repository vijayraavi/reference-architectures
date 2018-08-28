package com.microsoft.pnp


import java.sql.Timestamp

import org.apache.spark.eventhubs.{ConnectionStringBuilder, EventHubsConf, EventPosition}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout}

object TaxiCabReader {

  val taxiRideEventHubName = "taxi-ride"
  val taxiFareEventHubName = "taxi-fare"
  val sparkReadStreamFormatForEventHub = "eventhubs"
  val waterMarkTimeDuriation = "15 minutes"
  val maxThresholdBetweenStreams = "10 seconds"

  def main(args: Array[String]) {

    val spark = SparkSession.builder().config("spark.master", "local[10]").getOrCreate()
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
      .withWatermark("enqueueTime", waterMarkTimeDuriation)
      .as[EnrichedTaxiDataRecord]


    // Taxi fare read stream and extract fare object -
    val fareDataFrame = spark.readStream
      .format(sparkReadStreamFormatForEventHub)
      .options(fareEventHubConf.toMap)
      .load

    val fares = fareDataFrame
      .selectExpr("cast (body as string) AS fareContent",
        "enqueuedTime As recordIngestedTime")


    val taxiFareStream = fares.map(eventHubRow => TaxiFareMapper.mapRowToEncrichedTaxiFareRecord(eventHubRow))
      .withWatermark("enqueueTime", waterMarkTimeDuriation)
      .as[EnrichedTaxiDataRecord]


    val mergedTaxiDataRecords = taxiFareStream.union(taxiRideStream)

    val invalidTaxiDataRecords = mergedTaxiDataRecords.filter(record => !record.isValidRecord)

    val validTaxiDataRecords = mergedTaxiDataRecords.filter(record => record.isValidRecord)


    val groupedEnrichedTaxiDataRecords = validTaxiDataRecords.groupByKey(_.taxiDataRecordKey)
      .mapGroupsWithState(GroupStateTimeout.ProcessingTimeTimeout())(updateAcrossTaxiDataPerKeyRecords)
      .filter(groupedRecordWithState => groupedRecordWithState.expired)

    groupedEnrichedTaxiDataRecords.writeStream.outputMode("update").format("console").start().awaitTermination()

    spark.stop
  }


  case class TaxiDataPerKeyState(
                                  var taxiDataRecordKey: String,
                                  var startTaxiDataRecordsEnqueueTime: Timestamp,
                                  var endTaxiDataRecordsEnqueueTime: Timestamp,
                                  var numOfTaxiDataRecords: Int,
                                  var taxiRide: TaxiRide,
                                  var taxiFare: TaxiFare,
                                  var expired: Boolean)


  def updateTaxiDataPerKeyStateWithTaxiDataPerKeyRecord(state: TaxiDataPerKeyState, input: EnrichedTaxiDataRecord): TaxiDataPerKeyState = {

    var isDuplicateRecord = false
    if (state.taxiDataRecordKey == input.taxiDataRecordKey) {

      input.recordType match {
        case "taxiRide" =>
          if (state.taxiRide != null) {
            isDuplicateRecord = true
          } else {
            state.taxiRide = input.taxiRide
          }

        case "taxiFare" =>
          if (state.taxiFare != null) {
            isDuplicateRecord = true
          }
          else {
            state.taxiFare = input.taxiFare
          }
      }

      if (!isDuplicateRecord) {
        if (input.enqueueTime.after(state.endTaxiDataRecordsEnqueueTime)) {
          state.endTaxiDataRecordsEnqueueTime = input.enqueueTime
        }

        if (input.enqueueTime.before(state.startTaxiDataRecordsEnqueueTime)) {
          state.startTaxiDataRecordsEnqueueTime = input.enqueueTime
        }

        state.numOfTaxiDataRecords += 1
        state.expired = false
      }
    }
    else {

      if (input.enqueueTime.after(state.endTaxiDataRecordsEnqueueTime)) {
        state.startTaxiDataRecordsEnqueueTime = input.enqueueTime
        state.endTaxiDataRecordsEnqueueTime = input.enqueueTime
        state.taxiDataRecordKey = input.taxiDataRecordKey
        input.recordType match {
          case "taxiRide" => state.taxiRide = input.taxiRide
          case "taxiFare" => state.taxiFare = input.taxiFare
        }
        state.numOfTaxiDataRecords = 1
        state.expired = false
      }
    }

    state

  }


  def updateAcrossTaxiDataPerKeyRecords(taxiDataRecordKey: String, inputs: Iterator[EnrichedTaxiDataRecord],
                                        oldState: GroupState[TaxiDataPerKeyState]): TaxiDataPerKeyState = {


    if (oldState.hasTimedOut) {

      val currentState = oldState.get
      val newState = TaxiDataPerKeyState(
        currentState.taxiDataRecordKey,
        currentState.startTaxiDataRecordsEnqueueTime,
        currentState.endTaxiDataRecordsEnqueueTime,
        currentState.numOfTaxiDataRecords,
        currentState.taxiRide,
        currentState.taxiFare,
        !currentState.expired
      )
      oldState.remove()
      newState
    }
    else {

      var state: TaxiDataPerKeyState = if (oldState.exists) oldState.get
      else TaxiDataPerKeyState(
        "",
        new java.sql.Timestamp(6284160000000L),
        new java.sql.Timestamp(6284160L),
        0,
        null,
        null,
        false
      )

      for (input <- inputs) {

        state = updateTaxiDataPerKeyStateWithTaxiDataPerKeyRecord(state, input)
        oldState.update(state)
      }

      oldState.setTimeoutDuration(maxThresholdBetweenStreams)
      state
    }
  }


}
