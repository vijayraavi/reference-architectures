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

  def diff(t1input: Timestamp, t2input: Timestamp): Timestamp = { // Make sure the result is always > 0

    var t1 = t1input
    var t2 = t2input
    if (t1.compareTo(t2) < 0) {
      val tmp = t1
      t1 = t2
      t2 = tmp
    }
    // Timestamps mix milli and nanoseconds in the API, so we have to separate the two
    var diffSeconds = (t1.getTime / 1000) - (t2.getTime / 1000)
    // For normals dates, we have millisecond precision
    var nano1 = (t1.getTime.toInt % 1000) * 1000000
    // If the parameter is a Timestamp, we have additional precision in nanoseconds
    t1 match {
      case timestamp: Timestamp => nano1 = timestamp.getNanos
      case _ =>
    }
    var nano2 = (t2.getTime.toInt % 1000) * 1000000
    t2 match {
      case timestamp: Timestamp => nano2 = timestamp.getNanos
      case _ =>
    }
    var diffNanos = nano1 - nano2
    if (diffNanos < 0) { // Borrow one second
      diffSeconds -= 1
      diffNanos += 1000000000
    }
    // mix nanos and millis again
    val result = new java.sql.Timestamp((diffSeconds * 1000) + (diffNanos / 1000000))
    // setNanos() with a value of in the millisecond range doesn't affect the value of the time field
    // while milliseconds in the time field will modify nanos! Damn, this API is a *mess*
    result.setNanos(diffNanos)
    result
  }
}
