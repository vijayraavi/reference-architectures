package com.microsoft.pnp

case class TaxiFare(medallion: Long,
                    hackLicense: Long,
                    vendorId: String,
                    pickupTime: String,
                    paymentType: String,
                    fareAmount: Double,
                    surcharge: Double,
                    mTATax: Double,
                    tipAmount: Double,
                    tollsAmount: Double,
                    totalAmount: Double)
  extends TaxiData
    with Serializable
