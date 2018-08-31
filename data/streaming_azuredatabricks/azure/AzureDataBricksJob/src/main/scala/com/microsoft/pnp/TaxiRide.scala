package com.microsoft.pnp


case class TaxiRide(rateCode: Int,
                    storeAndForwardFlag: String,
                    dropoffTime: String,
                    passengerCount: Int,
                    tripTimeInSeconds: Double,
                    tripDistanceInMiles: Double,
                    pickupLon: Double,
                    pickupLat: Double,
                    dropoffLon: Double,
                    dropoffLat: Double,
                    medallion: Long,
                    hackLicense: Long,
                    vendorId: String,
                    pickupTime: String,
                    var neigbhourHood: String = "defaultNeighbourhood")
  extends TaxiData
    with Serializable


