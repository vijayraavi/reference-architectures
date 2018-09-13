package com.microsoft.pnp

import java.sql.Timestamp
import java.time.{ZoneId, ZonedDateTime}
import java.util.HashMap

import org.apache.spark.sql.Row
import org.apache.spark.sql.streaming.StreamingQueryListener.QueryProgressEvent
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.json4s.DefaultFormats

import scala.util.Try

object Utils {

  implicit val formats = DefaultFormats
  private val sourceEnqueTimeFormat: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS")

  def eventHubEnqueueTimeStringToTimeStamp(str: String): Option[Timestamp] = {

    Try(new Timestamp(DateTime.parse(str, sourceEnqueTimeFormat).getMillis)).toOption
  }
}
