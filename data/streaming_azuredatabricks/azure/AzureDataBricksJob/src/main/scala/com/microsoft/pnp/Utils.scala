package com.microsoft.pnp

import java.sql.Timestamp
import java.time.{ZoneId, ZonedDateTime}
import java.util.HashMap

import org.apache.spark.sql.Row
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

  def parseRow(row: Row) : HashMap[String,AnyRef] ={
    val date = java.time.format
      .DateTimeFormatter.RFC_1123_DATE_TIME.format(ZonedDateTime.now(ZoneId.of("GMT")))

    val bizmetrics = new HashMap[String, AnyRef]()

    for (i <- 0 until row.length) {
      bizmetrics.put(row.schema.fields(i).name,row(i).asInstanceOf[AnyRef])
    }

    bizmetrics.put("DateValue",date.toString())

    bizmetrics
  }
}
