package com.microsoft

import com.sun.org.apache.xalan.internal.xsltc.compiler.util.IntType
import org.apache.spark.sql.types._

package object pnp {

  val TaxiRideEventHubName = "taxi-ride"
  val TaxiFareEventHubName = "taxi-fare"
  val SparkStreamFormat = "eventhubs"
  val WaterMarkTimeDuration = "15 minutes"
  val MaxThresholdBetweenStreams = "50 seconds"
  val ErrorRecordsEventHubName = "taxi-ride" // Replace with error eventhub

  val RideSchema = new StructType()
    .add("rateCode", IntegerType)
    .add("storeAndForwardFlag", StringType)
    .add("dropoffTime",StringType)
    .add("passengerCount", IntegerType)
    .add("tripTimeInSeconds", DoubleType)
    .add("tripDistanceInMiles", DoubleType)
    .add("pickupLon", DoubleType)
    .add("pickupLat", DoubleType)
    .add("dropoffLon", DoubleType)
    .add("dropoffLat", DoubleType)
    .add("medallion", LongType)
    .add("hackLicense", LongType)
    .add("vendorId", StringType)
    .add("pickupTime", StringType)

  val FareSchema = new StructType()
    .add("medallion", LongType)
    .add("hackLicense", LongType)
    .add("vendorId",StringType)
    .add("pickupTime", StringType)
    .add("paymentType", StringType)
    .add("fareAmount", DoubleType)
    .add("surcharge", DoubleType)
    .add("mTATax", DoubleType)
    .add("tipAmount", DoubleType)
    .add("tollsAmount", DoubleType)
    .add("totalAmount", DoubleType)

  @transient lazy val NeighborhoodFinder = GeoFinder.createGeoFinder(NeighborhoodEnvReader.getNeighborhoodFileURL())
}

