package com.microsoft.pnp

import java.sql.Timestamp

case class EnrichedTaxiFareRecord(isValidFareRecord: Boolean,
                                  taxiFare: TaxiFare,
                                  fareExceptionMessage: String,
                                  actualFareRecord: String,
                                  fareRecordKey: String,
                                  fareEnqueueTime: Timestamp)
  extends Serializable

