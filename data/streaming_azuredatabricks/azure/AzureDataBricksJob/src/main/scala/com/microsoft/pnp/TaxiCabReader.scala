package com.microsoft.pnp

import java.sql.Timestamp

import com.microsoft.pnp.spark.StreamingMetricsListener
import org.apache.spark.SparkEnv
import org.apache.spark.metrics.source.{AppAccumulators, AppMetrics}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, Trigger}
import org.apache.spark.sql.Row
import scala.util.{Failure, Success, Try}
import org.apache.spark.sql.functions._
import com.google.gson.{Gson, JsonElement, JsonParser}

case class InputRow(medallion:String, hackLicense:String, vendorId:String, pickupTime:String, rateCode:String, storeAndForwardFlag:String, dropoffTime:String, passengerCount:String, tripTimeInSeconds:String, tripDistanceInMiles:String, pickupLon:String, pickupLat:String, dropoffLon:String, dropoffLat:String, paymentType:String, fareAmount:String, surcharge:String, mTATax:String, tipAmount:String, tollsAmount:String, totalAmount:String) extends Serializable

case class NeighborhoodState(neighborhoodName:String, var avgFarePerRide:Double, var ridesCount:Double) extends Serializable

case class TaxiRide(rateCode: Int, storeAndForwardFlag: String, dropoffTime: String, passengerCount: Int, tripTimeInSeconds: Double,
                    tripDistanceInMiles: Double, pickupLon: Double, pickupLat: Double, dropoffLon: Double, dropoffLat: Double,
                    medallion: Long, hackLicense: Long, vendorId: String, pickupTime: String) extends Serializable

case class TaxiFare(medallion: Long, hackLicense: Long, vendorId: String, pickupTime: String, paymentType: String,
                    fareAmount: Double, surcharge: Double, mTATax: Double, tipAmount: Double, tollsAmount: Double, totalAmount: Double) extends Serializable


object TaxiCabReader {

  def main(args: Array[String]) {

    /*    if (args.length < 2) {
          throw new Exception("Invalid Arguments")
        }

        val rideEventHubConnectionString = args(0)
        val fareEventHubConnectionString = args(1)*/

    val rideEventHubConnectionString = "Endpoint=sb://rs-eh-ns.servicebus.windows.net/;SharedAccessKeyName=taxi-ride-asa-access-policy;SharedAccessKey=/GjrSZc1uXKlwnrbXikYUEwcC++zE9nGJm4cRmvUlvw=;EntityPath=taxi-ride"
    val fareEventHubConnectionString = "Endpoint=sb://rs-eh-ns.servicebus.windows.net/;SharedAccessKeyName=taxi-fare-asa-access-policy;SharedAccessKey=eI7CeODrA/GZwNjPyn9E5PEGcXTLDqmK7C74CsU4tno=;EntityPath=taxi-fare"

    val spark = SparkSession.builder().config("spark.master", "local[10]").getOrCreate()

    @transient val appMetrics = new AppMetrics(spark.sparkContext)
    appMetrics.registerGauge("metricsregistrytest.processed",
      AppAccumulators.getProcessedInputCountInstance(spark.sparkContext))
    SparkEnv.get.metricsSystem.registerSource(appMetrics)

    import spark.implicits._

    val csvFareToJson = (farePayload: String) => {
      val csvString = farePayload.split("\r?\n")(1)
      val tokens = csvString.split(",").map(x => x.trim)

      val splitPickUpTimes = tokens(3).split(" ").map(x => x.trim)

      val medallion = tokens(0).toLong
      val hackLicense = tokens(1).toLong
      val vendorId = tokens(2)
      val pickupTime = splitPickUpTimes(0) + "T" + splitPickUpTimes(1) + "+00:00"
      val paymentType = tokens(4)
      val fareAmount = tokens(5).toDouble
      val surcharge = tokens(6).toDouble
      val mTATax = tokens(7).toDouble
      val tipAmount = tokens(8).toDouble
      val tollsAmount = tokens(9).toDouble
      val totalAmount = tokens(10).toDouble

      TaxiFare(medallion, hackLicense, vendorId, pickupTime, paymentType
        , fareAmount, surcharge, mTATax, tipAmount, tollsAmount, totalAmount)
    }

    val neighborhoodFinder = (lat: Double, lon: Double ) => {
      NeighborhoodFinder.getNeighborhood(lon, lat).get()
    }

    val to_neighborhood = spark.udf.register("neighborhoodFinder", neighborhoodFinder)
    val myUDF = spark.udf.register("csvFareToJson", csvFareToJson)


    spark.streams.addListener(new StreamingMetricsListener())

    val rideEvents = EventHubStream.ExecuteDataFrame(spark,
      EventHubStream.BuildConnectionString(rideEventHubConnectionString, TaxiRideEventHubName))

    val fareEvents = EventHubStream.ExecuteDataFrame(spark,
      EventHubStream.BuildConnectionString(fareEventHubConnectionString, TaxiFareEventHubName))

    val rides = rideEvents
      .selectExpr("CAST(body AS STRING) AS ridePayload", "enqueuedTime As rideTime")
      .filter(r => isValidRideEvent(r))
      .select($"rideTime", from_json($"ridePayload", RideSchema) as "rides")
      .select($"rideTime", $"rides.*", to_neighborhood($"rides.pickupLon", $"rides.pickupLat"))
      .withWatermark("rideTime", "1 seconds")


    val fares = fareEvents
      .selectExpr("CAST(body AS STRING) AS farePayload", "enqueuedTime As fareTime")
      .filter(r => isValidFareEvent(r))
      .withColumn("fares", myUDF($"farePayload"))
      .select($"fareTime", $"fares.*")
      .withWatermark("fareTime", "1 seconds")

    val mergedTaxiTrip = rides.join(fares, Seq("medallion", "hackLicense", "vendorId", "pickupTime"))

    val maxAvgFarePerNeighborhood =  mergedTaxiTrip.selectExpr("medallion", "hackLicense", "vendorId", "pickupTime", "rateCode", "storeAndForwardFlag", "dropoffTime", "passengerCount", "tripTimeInSeconds", "tripDistanceInMiles", "pickupLon", "pickupLat", "dropoffLon", "dropoffLat", "paymentType", "fareAmount", "surcharge", "mTATax", "tipAmount", "tollsAmount", "totalAmount")
      .as[InputRow]
      .groupByKey(_.passengerCount)
      .mapGroupsWithState(GroupStateTimeout.ProcessingTimeTimeout)(updateForEvents)
      .writeStream
      .queryName("events_per_window")
      .format("memory")
      .outputMode("update")
      .start()

  }

  def parseDouble(s: String): Option[Double] = Try { s.toDouble }.toOption

  def updateNeighborhoodStateWithEvent(state:NeighborhoodState, input:InputRow):NeighborhoodState = {
    if (parseDouble(input.fareAmount) == None) {
      return state
    }

    val fare = parseDouble(input.fareAmount).get

    state.avgFarePerRide = ((state.avgFarePerRide * state.ridesCount) + fare)/ (state.ridesCount + 1)
    state.ridesCount += 1

    state
  }

  def updateForEvents(neighborhoodName:String,
                      inputs: Iterator[InputRow],
                      oldState: GroupState[NeighborhoodState]):NeighborhoodState = {

    var state:NeighborhoodState = if (oldState.exists) oldState.get else NeighborhoodState(neighborhoodName, 0, 0)

    for (input <- inputs) {
      state = updateNeighborhoodStateWithEvent(state, input)
      oldState.update(state)
    }

    state
  }

  def isValidRideEvent(eventHubRow: Row) : Boolean = {
    val gson = new Gson()

    Try(gson.fromJson(eventHubRow(0).toString, classOf[TaxiRide])) match {
      case Success(r) => true
      case Failure(exception) => false
    }
  }

  def isValidFareEvent(eventHubRow: Row) : Boolean = {
    val farePayload = eventHubRow(0).toString
    try {
      val csvString = farePayload.split("\r?\n")(1)
      val tokens = csvString.split(",").map(x => x.trim)

      if (tokens.length != 11) {
        throw new Exception("invalid taxi fare csv")
      }
      val splitPickUpTimes = tokens(3).split(" ").map(x => x.trim)

      val medallion = tokens(0).toLong
      val hackLicense = tokens(1).toLong
      val vendorId = tokens(2)
      val pickupTime = splitPickUpTimes(0) + "T" + splitPickUpTimes(1) + "+00:00"
      val paymentType = tokens(4)
      val fareAmount = tokens(5).toDouble
      val surcharge = tokens(6).toDouble
      val mTATax = tokens(7).toDouble
      val tipAmount = tokens(8).toDouble
      val tollsAmount = tokens(9).toDouble
      val totalAmount = tokens(10).toDouble

      val fare = TaxiFare(medallion, hackLicense, vendorId, pickupTime, paymentType
        , fareAmount, surcharge, mTATax, tipAmount, tollsAmount, totalAmount)
    }
    catch {
      case ex: Exception => false
    }

    true
  }
}
