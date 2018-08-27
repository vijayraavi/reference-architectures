package com.microsoft.pnp

import java.sql.Timestamp

import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

import scala.util.Try


object Utils {

  private val sourceEnqueTimeFormat: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS")

  def eventHubEnqueueTimeStringToTimeStamp(str: String): Option[Timestamp] = {

    Try(new Timestamp(DateTime.parse(str, sourceEnqueTimeFormat).getMillis)).toOption
  }

}
