package com.microsoft.pnp


import java.sql.Timestamp

import org.apache.spark.eventhubs.{ConnectionStringBuilder, EventHubsConf, EventPosition}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout}

import scala.util.{Failure, Success, Try}

object TaxiCabReader {

  val taxiRideEventHubName = "taxi-ride"
  val taxiFareEventHubName = "taxi-fare"
  val sparkStreamFormatForEventHub = "eventhubs"
  val groupedRecordsEventHubName = "groupedenrichedtaxidatarecords"
  val waterMarkTimeDuriation = "15 minutes"
  val maxThresholdBetweenStreams = "50 seconds"
  val errorRecordsEventHubName = "taxi-ride" //TODO get one defined. time sbeing using an existing one

  def main(args: Array[String]) {


    Try(parseArgs(args)) match {
      case Success(arguments) => {

        val spark = SparkSession.builder().config("spark.master", "local[10]").getOrCreate()

        //        @transient val appMetrics = new AppMetrics(spark.sparkContext)
        //        appMetrics.registerGauge("metricsregistrytest.processed",
        //          AppAccumulators.getProcessedInputCountInstance(spark.sparkContext))
        //        SparkEnv.get.metricsSystem.registerSource(appMetrics)

        import spark.implicits._


        //        spark.streams.addListener(new StreamingMetricsListener())
        // Taxi ride infra setup -

        val rideConnectionString = ConnectionStringBuilder(arguments.rideEventHubConnectionString)
          .setEventHubName(taxiRideEventHubName)
          .build

        val rideEventHubConf = EventHubsConf(rideConnectionString)
          .setStartingPosition(EventPosition.fromStartOfStream)

        //Taxi fare infra setup -

        val fareConnectionString = ConnectionStringBuilder(arguments.fareEventHubConnectionString)
          .setEventHubName(taxiFareEventHubName)
          .build

        val fareEventHubConf = EventHubsConf(fareConnectionString)
          .setStartingPosition(EventPosition.fromStartOfStream)

        //Error records infra setup  -

        val errorRecordsEventHubConnectionString = ConnectionStringBuilder(arguments.errorRecordsEventHubConnectionString)
          .setEventHubName(errorRecordsEventHubName)
          .build

        val errorEventHubConf = EventHubsConf(errorRecordsEventHubConnectionString)
          .setStartingPosition(EventPosition.fromEndOfStream)

        // grouped records infra set up -

        val groupedRecordsEventHubConnectionString = ConnectionStringBuilder(arguments.groupedEnrichedTaxiDataRecordsEventHubConnectionString)
          .setEventHubName(groupedRecordsEventHubName)
          .build

        val groupedRecordsEventHubConf = EventHubsConf(groupedRecordsEventHubConnectionString)
          .setStartingPosition(EventPosition.fromEndOfStream)


        //Taxi ride read stream and extract taxi ride object -
        val rideDataFrame = spark.readStream
          .format(sparkStreamFormatForEventHub)
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
          .format(sparkStreamFormatForEventHub)
          .options(fareEventHubConf.toMap)
          .load

        val fares = fareDataFrame
          .selectExpr("cast (body as string) AS fareContent",
            "enqueuedTime As recordIngestedTime")


        val taxiFareStream = fares.map(eventHubRow => TaxiFareMapper.mapRowToEncrichedTaxiFareRecord(eventHubRow))
          .withWatermark("enqueueTime", waterMarkTimeDuriation)
          .as[EnrichedTaxiDataRecord]


        val mergedTaxiDataRecords = taxiFareStream.union(taxiRideStream)

        //        val invalidTaxiDataRecords = mergedTaxiDataRecords.filter(record => !record.isValidRecord)
        //        invalidTaxiDataRecords
        //          .writeStream
        //          .format(sparkStreamFormatForEventHub)
        //          .outputMode("update")
        //          .options(errorEventHubConf.toMap)
        //          .trigger(Trigger.ProcessingTime("25 seconds"))
        //          .option("checkpointLocation", "./checkpoint/") // TODO paramterize checkpoint location and make it point to azure blob
        //          .start()
        //                  .awaitTermination()

        // option 1
        // we push the # of invalid taxi records to ALA
        // as a business metrics
        // using a business metris writer for high scala and performance
        //        val businessMetricsWriter = new BusinessMetricsWriter()
        //        invalidTaxiDataRecords.toDF()
        //          .groupBy("isValidRecord")
        //          .agg(
        //            count(lit(1)).alias("CountInvalidRecords")
        //          )
        //          .writeStream
        //          .foreach(businessMetricsWriter)
        //          .outputMode("append")
        //          .start()
        //          .awaitTermination()
        //option 2
        // we use the accumulator to pile up the counts of
        // invalid records
        // metrics registry will log to ALA
        //        val processedInputCount = AppAccumulators.getProcessedInputCountInstance(spark.sparkContext)
        //        processedInputCount.add(invalidTaxiDataRecords.count())

        val validTaxiDataRecords = mergedTaxiDataRecords.filter(record => record.isValidRecord)

        val groupedEnrichedTaxiDataRecords = validTaxiDataRecords.groupByKey(_.taxiDataRecordKey)
          .mapGroupsWithState(GroupStateTimeout.ProcessingTimeTimeout())(updateAcrossTaxiDataPerKeyRecords)
          .filter(groupedRecordWithState => groupedRecordWithState.expired)
          .as[TaxiDataPerKeyState]


        val dropOffRecords = groupedEnrichedTaxiDataRecords
          .map(x => DropOffRecordMapper.mapEnrichedTaxiDataRecordToDropOffRecord(x))
          .as[DropOffRecord]

        dropOffRecords.toDF("neighborhood", "eventTime", "eventDay", "eventHour", "eventMin", "event15MinWindow", "key")
          .select(to_json(struct($"neighborhood"
            , $"eventTime"
            , $"eventDay"
            , $"eventHour"
            , $"eventMin"
            , $"event15MinWindow"
            , $"key"
          )).as("body"))
          .writeStream
          .format(sparkStreamFormatForEventHub)
          .options(groupedRecordsEventHubConf.toMap)
          .outputMode("update")
          .option("checkpointLocation", "./groupedRecordscheckpoint/")
          .start()
          .awaitTermination()

        spark.stop
      }

      case Failure(exception) => println(exception.getLocalizedMessage)
    }


  }


  case class TaxiDataPerKeyState(
                                  var taxiDataRecordKey: String,
                                  var startTaxiDataRecordsEnqueueTime: Timestamp,
                                  var endTaxiDataRecordsEnqueueTime: Timestamp,
                                  var numOfTaxiDataRecords: Int,
                                  var taxiRide: TaxiRide,
                                  var taxiFare: TaxiFare,
                                  var expired: Boolean)
    extends Serializable


  case class DropOffNeighborhoodState(
                                       var dropOffRecordKey: String,
                                       var dropOffNeighborhood: String,
                                       var eventWindow: String,
                                       var distinctDaysSeen: Int,
                                       var count: Long,
                                       var currentDay: String,
                                       var avg: Float
                                     )


  case class Arguments(
                        rideEventHubConnectionString: String,
                        fareEventHubConnectionString: String,
                        errorRecordsEventHubConnectionString: String,
                        groupedEnrichedTaxiDataRecordsEventHubConnectionString: String
                      )


  def parseArgs(args: Array[String]): Arguments = {


    if (args.length < 3) {
      throw new Exception("please provide valid arguments for this program")
    }

    Arguments(
      args(0),
      args(1),
      args(2),
      args(3)
    )

  }

  def updateDropOffNeighborhoodStateWithDropOffNeibhorHoodRecord(state: DropOffNeighborhoodState, input: DropOffRecord): DropOffNeighborhoodState = {

    if (state.dropOffRecordKey == input.key) {

      state.dropOffNeighborhood = input.neighborhood
      state.count = state.count + 1
      state.eventWindow = input.event15MinWindow
      if (state.currentDay != input.eventDay) {
        state.distinctDaysSeen = state.distinctDaysSeen + 1
      }
      state.avg = state.count / state.distinctDaysSeen


    }
    else {

      state.dropOffNeighborhood = input.neighborhood
      state.count = state.count + 1
      state.currentDay = input.eventDay
      state.distinctDaysSeen = 1
      state.avg = 1 // count/distinctDaysseen = 1/1
      state.eventWindow = input.event15MinWindow
    }

    state
  }


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


  def updateAcrossDropOffRecordsPerNeighborhood(dropOffNeighborhood: String, inputs: Iterator[DropOffRecord],
                                                oldState: GroupState[DropOffNeighborhoodState]): DropOffNeighborhoodState = {


    var state: DropOffNeighborhoodState = if (oldState.exists) oldState.get
    else DropOffNeighborhoodState(
      "",
      "",
      "",
      0,
      0L,
      "",
      0.0f
    )

    for (input <- inputs) {
      state = updateDropOffNeighborhoodStateWithDropOffNeibhorHoodRecord(state, input)
      oldState.update(state)
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
        expired = false
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
