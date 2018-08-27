package com.microsoft.pnp

import java.sql.Timestamp

case class EnrichedTaxiRideRecord(isValidRideRecord: Boolean,
                                  taxiRide: TaxiRide,
                                  rideExceptionMessage: String,
                                  actualRideRecord: String,
                                  rideRecordKey: String,
                                  rideEnqueueTime: Timestamp)
  extends Serializable
