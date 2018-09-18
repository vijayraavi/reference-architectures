package com.microsoft.pnp

import com.microsoft.pnp.TaxiCabReader.TaxiDataPerKeyState

case class DropOffRecord(
                          neighborhood: String,
                          eventTime: String,
                          eventDay: String,
                          eventHour: String,
                          eventMin: String,
                          event15MinWindow: String,
                          key: String
                        )
  extends Serializable

object DropOffRecordMapper {


  def mapEnrichedTaxiDataRecordToDropOffRecord(taxiDataPerKeyState: TaxiDataPerKeyState): DropOffRecord = {

    val taxiRide = taxiDataPerKeyState.taxiRide
    val dropTimeSplit = taxiRide.dropoffTime.split("T")
    val neighborhood = taxiRide.dropOffNeigbhorHood
    val eventTime = taxiRide.dropoffTime
    val eventDay = dropTimeSplit(0)
    val eventHour = dropTimeSplit(1).substring(0, 2)
    val eventMin = dropTimeSplit(1).split(":")(1).substring(0, 2)

    val longEventMin = Integer.parseInt(eventMin)
    var event15MinWindow = ""
    longEventMin match {

      case it if 0 until 15 contains it => event15MinWindow = eventHour + ":15"
      case it if 15 until 30 contains it => event15MinWindow = eventHour + ":30"
      case it if 30 until 45 contains it => event15MinWindow = eventHour + ":45"
      case it if 45 until 60 contains it => {

        var boundedHour = (Integer.parseInt(eventHour) + 1) % 24
        if (boundedHour < 10) {
          event15MinWindow = "0" + boundedHour + ":00"
        }
        else {
          event15MinWindow = boundedHour + ":00"
        }

      }

    }

    DropOffRecord(
      neighborhood,
      eventTime,
      eventDay,
      eventHour,
      eventMin,
      event15MinWindow,
      taxiDataPerKeyState.taxiDataRecordKey + "_" + neighborhood + "_" + event15MinWindow

    )

  }

}
