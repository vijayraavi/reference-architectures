package com.microsoft.pnp

import java.sql.Timestamp

case class EnrichedTaxiDataRecord(isValidRecord: Boolean,
                                  taxiFare: TaxiFare,
                                  taxiRide: TaxiRide,
                                  exceptionMessage: String,
                                  inputRecord: String,
                                  taxiDataRecordKey: String,
                                  enqueueTime: Timestamp,
                                  recordType: String
                                 )
  extends Serializable
