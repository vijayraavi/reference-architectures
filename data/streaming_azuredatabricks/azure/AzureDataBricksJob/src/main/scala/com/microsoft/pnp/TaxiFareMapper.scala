package com.microsoft.pnp


import org.apache.spark.sql.Row

import scala.util.{Failure, Success, Try}

object TaxiFareMapper {

  val invalidTaxiFareCsv = "invalid taxi fare csv"

  def mapRowToEncrichedTaxiFareRecord(eventHubRow: Row): EnrichedTaxiFareRecord = {

    validateHeaderEmbededCsvString(eventHubRow(0).toString) match {
      case Success(csvContent) => Try(mapCsvToTaxiFare(csvContent)) match {

        case Success(taxiFare) => EnrichedTaxiFareRecord(
          isValidFareRecord = true,
          taxiFare,
          "",
          eventHubRow(0).toString,
          taxiFare.key,
          Utils.eventHubEnqueueTimeStringToTimeStamp(eventHubRow(1).toString).get)
        case Failure(exception) => EnrichedTaxiFareRecord(
          isValidFareRecord = false,
          null,
          exception.getMessage,
          eventHubRow(0).toString,
          "",
          Utils.eventHubEnqueueTimeStringToTimeStamp(eventHubRow(1).toString).get)
      }
      case Failure(exception) => EnrichedTaxiFareRecord(
        isValidFareRecord = false,
        null,
        exception.getMessage,
        eventHubRow(0).toString,
        "",
        Utils.eventHubEnqueueTimeStringToTimeStamp(eventHubRow(1).toString).get)
    }
  }


  def validateHeaderEmbededCsvString(csvString: String): Try[String] = {
    Try(csvString.split("\r?\n")(1))
  }


  def mapCsvToTaxiFare(csvString: String): TaxiFare = {
    try {
      val tokens = csvString.split(",").map(x => x.trim)

      if (tokens.length != 11) {
        throw new Exception(invalidTaxiFareCsv)
      }

      val medallion = tokens(0).toLong
      val hackLicense = tokens(1).toLong
      val vendorId = tokens(2)
      val pickupTime = mapFarePickUpTimeToRidePickUpTimeFormat(tokens(3))
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
    catch {
      case e: Exception => throw e
    }


  }


  def mapFarePickUpTimeToRidePickUpTimeFormat(input: String): String = {
    val splitPickUpTimes = input.split(" ").map(x => x.trim)
    splitPickUpTimes(0) + "T" + splitPickUpTimes(1) + "+00:00"
  }
}
