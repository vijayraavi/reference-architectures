package com.microsoft.pnp

import java.sql.Timestamp

import com.microsoft.pnp.spark.StreamingMetricsListener
import org.apache.spark.SparkEnv
import org.apache.spark.eventhubs.{EventHubsConf, EventPosition}
import org.apache.spark.metrics.source.{AppAccumulators, AppMetrics}
import org.apache.spark.sql.{Column, Row, SparkSession}
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}

import scala.util.Try
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.expressions.{CsvToStructs, Expression}
import org.apache.spark.sql.types.{StringType, StructType}

case class InputRow(
                     medallion: Long,
                     hackLicense: Long,
                     vendorId: String,
                     pickupTime: Timestamp,
                     rateCode: Int,
                     storeAndForwardFlag: String,
                     dropoffTime: Timestamp,
                     passengerCount: Int,
                     tripTimeInSeconds: Double,
                     tripDistanceInMiles: Double,
                     pickupLon: Double,
                     pickupLat: Double,
                     dropoffLon: Double,
                     dropoffLat: Double,
                     paymentType: String,
                     fareAmount: Double,
                     surcharge: Double,
                     mtaTax: Double,
                     tipAmount: Double,
                     tollsAmount: Double,
                     totalAmount: Double,
                     pickupNeighborhood: String,
                     dropoffNeighborhood: String) extends Serializable

case class NeighborhoodState(neighborhoodName:String, var avgFarePerRide:Double, var ridesCount:Double) extends Serializable

object TaxiCabReader {
  private def withExpr(expr: Expression): Column = new Column(expr)

  def main(args: Array[String]) {
    val conf = new JobConfiguration(args)
    val rideEventHubConnectionString = getSecret(
      conf.secretScope(), conf.taxiRideEventHubSecretName())
    val fareEventHubConnectionString = getSecret(
      conf.secretScope(), conf.taxiFareEventHubSecretName())

    // DBFS root for our job
    val dbfsRoot = "dbfs:/azure-databricks-job"
    val checkpointRoot = s"${dbfsRoot}/checkpoint"
    val malformedRoot = s"${dbfsRoot}/malformed"

    val spark = SparkSession.builder().config("spark.master", "local[10]").getOrCreate()
    import spark.implicits._

    @transient val appMetrics = new AppMetrics(spark.sparkContext)
    appMetrics.registerGauge("metrics.invalidrecords",
      AppAccumulators.getProcessedInputCountInstance(spark.sparkContext))
    SparkEnv.get.metricsSystem.registerSource(appMetrics)

    @transient lazy val NeighborhoodFinder = GeoFinder.createGeoFinder(
      conf.neighborhoodFileURL())
    val neighborhoodFinder = (lon: Double, lat: Double ) => {
      NeighborhoodFinder.getNeighborhood(lon, lat).get()
    }
    val to_neighborhood = spark.udf.register("neighborhoodFinder", neighborhoodFinder)

    def from_csv(e: Column, schema: StructType, options: Map[String, String]): Column = withExpr {
      CsvToStructs(schema, options, e.expr)
    }

    spark.streams.addListener(new StreamingMetricsListener())

    val rideEventHubOptions = EventHubsConf(rideEventHubConnectionString)
      .setConsumerGroup(conf.taxiRideConsumerGroup())
      .setStartingPosition(EventPosition.fromStartOfStream)
    val rideEvents = spark.readStream
      .format("eventhubs")
      .options(rideEventHubOptions.toMap)
      .load

    val fareEventHubOptions = EventHubsConf(fareEventHubConnectionString)
      .setConsumerGroup(conf.taxiFareConsumerGroup())
      .setStartingPosition(EventPosition.fromStartOfStream)
    val fareEvents = spark.readStream
      .format("eventhubs")
      .options(fareEventHubOptions.toMap)
      .load

    val transformedRides = rideEvents
      .select(
        $"body"
          .cast(StringType)
          .as("messageData"),
        from_json($"body".cast(StringType), RideSchema)
          .as("ride"),
        $"enqueuedTime"
          .as("rideTime"))
      .transform(ds => {
        ds.withColumn(
          "errorMessage",
          when($"ride".isNull,
            lit("Error decoding JSON"))
          .otherwise(lit(null))
        )
      })

    // add rides invalid records as an accumulator
    // it is processed by metrics registry
    // it is registered in SparkMetric_CL

    var accInvalidRecords = AppAccumulators.getProcessedInputCountInstance(spark.sparkContext)
    accInvalidRecords.add(transformedRides
      .filter($"errorMessage".isNotNull)
      .select($"messageData")
      .count())

      val invalidRides = transformedRides
          .filter($"errorMessage".isNotNull)
        .select($"messageData")
        .writeStream
        .outputMode(OutputMode.Append)
        .queryName("invalid_ride_records")
        .format("csv")
        .option("path", s"${malformedRoot}/rides")
        .option("checkpointLocation", s"${checkpointRoot}/rides")


      val rides = transformedRides
          .filter($"errorMessage".isNull)
        .select(
          $"rideTime",
          $"ride.*",
          to_neighborhood($"ride.pickupLon", $"ride.pickupLat")
            .as("pickupNeighborhood"),
          to_neighborhood($"ride.dropoffLon", $"ride.dropoffLat")
            .as("dropoffNeighborhood")
        )
        .withWatermark("pickupTime", conf.taxiRideWatermarkInterval())

    val csvOptions = Map("header" -> "true", "multiLine" -> "true")
    val transformedFares = fareEvents
      .select(
        $"body"
          .cast(StringType)
          .as("messageData"),
        from_csv($"body".cast(StringType), FareSchema, csvOptions)
          .as("fare"),
        $"enqueuedTime"
          .as("rideTime"))
        .transform(ds => {
          ds.withColumn(
            "errorMessage",
            when($"fare".isNull,
              lit("Error decoding CSV"))
            .when(to_timestamp($"fare.pickupTimeString", "yyyy-MM-dd HH:mm:ss").isNull,
              lit("Error parsing pickupTime"))
            .otherwise(lit(null))
          )
        })
      .transform(ds => {
        ds.withColumn(
          "pickupTime",
          when($"fare".isNull,
            lit(null))
            .otherwise(to_timestamp($"fare.pickupTimeString", "yyyy-MM-dd HH:mm:ss"))
        )
      })

    // add fare invalid records as an accumulator. it is
    // accumulated with ride invalid records
    // it is processed by metrics registry
    // it is registered in SparkMetric_CL

    accInvalidRecords = AppAccumulators.getProcessedInputCountInstance(spark.sparkContext)
    accInvalidRecords.add(transformedFares
      .filter($"errorMessage".isNotNull)
      .select($"messageData")
      .count())


    val invalidFares = transformedFares
      .filter($"errorMessage".isNotNull)
      .select($"messageData")
      .writeStream
      .outputMode(OutputMode.Append)
      .queryName("invalid_fare_records")
      .format("csv")
      .option("path", s"${malformedRoot}/fares")
      .option("checkpointLocation", s"${checkpointRoot}/fares")






        val fares = transformedFares
            .filter($"errorMessage".isNull)
          .select(
            $"rideTime",
            $"fare.*",
            $"pickupTime"
          )
          .withWatermark("pickupTime", conf.taxiFareWatermarkInterval())

    val mergedTaxiTrip = rides.join(fares, Seq("medallion", "hackLicense", "vendorId", "pickupTime"))

    val maxAvgFarePerNeighborhoodDf =  mergedTaxiTrip.selectExpr("medallion", "hackLicense", "vendorId", "pickupTime", "rateCode", "storeAndForwardFlag", "dropoffTime", "passengerCount", "tripTimeInSeconds", "tripDistanceInMiles", "pickupLon", "pickupLat", "dropoffLon", "dropoffLat", "paymentType", "fareAmount", "surcharge", "mtaTax", "tipAmount", "tollsAmount", "totalAmount", "pickupNeighborhood", "dropoffNeighborhood")
      .as[InputRow]
      .groupBy(window($"pickupTime", conf.windowInterval()), $"pickupNeighborhood")
      .agg(
        count("*").as("rideCount"),
        sum($"fareAmount").as("totalFareAmount"),
        sum($"tipAmount").as("totalTipAmount")
      )
      .select($"window.start", $"window.end", $"pickupNeighborhood", $"rideCount", $"totalFareAmount", $"totalTipAmount")

    maxAvgFarePerNeighborhoodDf
      .writeStream
      .outputMode(OutputMode.Append)
      .queryName("events_per_window")
      .format("console")
      .start

    // send telemetry of pickupNeighborhood, ridecount, totalfareAmount and totalTipAmount
    // as business metrics. log type it is configured in type of log4j.properties

    val businessMetricsWriter = new BusinessMetricsWriter()

    maxAvgFarePerNeighborhoodDf
      .writeStream
      .foreach(businessMetricsWriter)
      .outputMode("append")
      .start()
      .awaitTermination()

    invalidRides
      .start
    invalidFares
      .start
  }

  def updateNeighborhoodStateWithEvent(state:NeighborhoodState, input:InputRow):NeighborhoodState = {
    state.avgFarePerRide = ((state.avgFarePerRide * state.ridesCount) + input.fareAmount) / (state.ridesCount + 1)
    state.ridesCount += 1
    state
  }

  def updateForEvents(neighborhoodName: String,
                      inputs: Iterator[InputRow],
                      oldState: GroupState[NeighborhoodState]): Iterator[NeighborhoodState] = {

    var state:NeighborhoodState = if (oldState.exists) oldState.get else NeighborhoodState(neighborhoodName, 0, 0)

    for (input <- inputs) {
      state = updateNeighborhoodStateWithEvent(state, input)
      oldState.update(state)
    }

    Iterator(state)
  }
}
