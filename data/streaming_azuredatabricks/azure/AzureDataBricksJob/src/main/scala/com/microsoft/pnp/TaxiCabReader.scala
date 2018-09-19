package com.microsoft.pnp


import java.sql.Timestamp

import com.microsoft.pnp.spark.StreamingMetricsListener
import org.apache.spark.SparkEnv
import org.apache.spark.eventhubs.{ConnectionStringBuilder, EventHubsConf, EventPosition}
import org.apache.spark.metrics.source.{AppAccumulators, AppMetrics}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{count, lit}
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, Trigger}

import scala.util.{Failure, Success, Try}

object TaxiCabReader {

  val taxiRideEventHubName = "taxi-ride"
  val taxiFareEventHubName = "taxi-fare"
  val sparkStreamFormatForEventHub = "eventhubs"
  val waterMarkTimeDuriation = "15 minutes"
  val maxThresholdBetweenStreams = "50 seconds"
  val errorRecordsEventHubName = "taxi-ride" //TODO get one defined. time sbeing using an existing one

  def main(args: Array[String]) {


    Try(parseArgs(args)) match {
      case Success(arguments) => {

        val spark = SparkSession.builder().config("spark.master", "local[10]").getOrCreate()

        @transient val appMetrics = new AppMetrics(spark.sparkContext)
        appMetrics.registerGauge("metricsregistrytest.processed",
          AppAccumulators.getProcessedInputCountInstance(spark.sparkContext))
        SparkEnv.get.metricsSystem.registerSource(appMetrics)

        import spark.implicits._


        spark.streams.addListener(new StreamingMetricsListener())
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

        val invalidTaxiDataRecords = mergedTaxiDataRecords.filter(record => !record.isValidRecord)


        // option 1
        // we push the # of invalid taxi records to ALA
        // as a business metrics
        // using a business metris writer for high scala and performance
        val businessMetricsWriter = new BusinessMetricsWriter()
        invalidTaxiDataRecords.toDF()
          .groupBy("isValidRecord")
          .agg(
            count(lit(1)).alias("CountInvalidRecords")
          )
          .writeStream
          .foreach(businessMetricsWriter)
          .outputMode("append")
          .start()
          .awaitTermination()
        //option 2
        // we use the accumulator to pile up the counts of
        // invalid records
        // metrics registry will log to ALA
        val processedInputCount = AppAccumulators.getProcessedInputCountInstance(spark.sparkContext)
        processedInputCount.add(invalidTaxiDataRecords.count())

        val validTaxiDataRecords = mergedTaxiDataRecords.filter(record => record.isValidRecord)


        val groupedEnrichedTaxiDataRecords = validTaxiDataRecords.groupByKey(_.taxiDataRecordKey)
          .mapGroupsWithState(GroupStateTimeout.ProcessingTimeTimeout())(updateAcrossTaxiDataPerKeyRecords)
          .filter(groupedRecordWithState => groupedRecordWithState.expired)


        invalidTaxiDataRecords
          .writeStream
          .format(sparkStreamFormatForEventHub)
          .outputMode("update")
          .options(errorEventHubConf.toMap)
          .trigger(Trigger.ProcessingTime("25 seconds"))
          .option("checkpointLocation", "./checkpoint/") // TODO paramterize checkpoint location and make it point to azure blob
          .start()


//        val neibhourhoods = groupedEnrichedTaxiDataRecords.filter(x => x.taxiRide != null).map(x => x.taxiRide.neigbhourHood)


        groupedEnrichedTaxiDataRecords
          .writeStream
          .outputMode("update")
          .format("console")
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


  case class Arguments(
                        rideEventHubConnectionString: String,
                        fareEventHubConnectionString: String,
                        errorRecordsEventHubConnectionString: String
                      )


  def parseArgs(args: Array[String]): Arguments = {


    if (args.length < 3) {
      throw new Exception("please provide valid arguments for this program")
    }

    Arguments(
      args(0),
      args(1),
      args(2)
    )

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
