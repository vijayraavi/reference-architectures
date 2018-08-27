package com.microsoft.pnp

abstract class TaxiData {

  def medallion: Long

  def hackLicense: Long

  def vendorId: String

  def pickupTime: String

  def key: String = {

    "%s_%s_%s_%s".format(medallion, hackLicense, vendorId, pickupTime)
  }
}
