package com.microsoft.pnp

import java.sql.Timestamp
import java.time.{ZoneId, ZonedDateTime}
import java.util
import java.util.HashMap

import org.apache.spark.sql.Row
import org.apache.spark.sql.streaming.StreamingQueryListener.QueryProgressEvent
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.json4s.{DefaultFormats, JObject}

import scala.util.Try


object Utils {

  implicit val formats = DefaultFormats
  private val sourceEnqueTimeFormat: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS")

  def eventHubEnqueueTimeStringToTimeStamp(str: String): Option[Timestamp] = {

    Try(new Timestamp(DateTime.parse(str, sourceEnqueTimeFormat).getMillis)).toOption
  }


  def parsePayload(event: QueryProgressEvent): util.HashMap[String, AnyRef] = {
    val date = java.time.format
      .DateTimeFormatter.RFC_1123_DATE_TIME.format(ZonedDateTime.now(ZoneId.of("GMT")))

    var dataJson = org.json4s.native.JsonParser.parse(event.progress.json).asInstanceOf[JObject]
    var dataValue = dataJson \ "durationMs" \ "triggerExecution"
    val triggerExecution: Double = dataValue.extract[Double]

    dataValue = dataJson \ "durationMs" \ "getBatch"
    val getBatch: Double = dataValue.extract[Double]

    dataValue = dataJson \ "id"
    val id: String = dataValue.extract[String]

    dataValue = dataJson \ "numInputRows"
    val inputRows: Integer = dataValue.extract[Integer]

    dataValue = dataJson \ "sink" \ "description"
    val sink: String = dataValue.extract[String]

    dataValue = dataJson \ "inputRowsPerSecond"
    val inRowsPerSecond: Double = dataValue.extract[Double]

    dataValue = dataJson \ "processedRowsPerSecond"
    val procRowsPerSecond: Double = dataValue.extract[Double]

    val metrics = new util.HashMap[String, AnyRef]()

    metrics.put("name", event.progress.name)
    metrics.put("id", id)
    metrics.put("sink", sink)
    metrics.put("triggerExecution", triggerExecution.asInstanceOf[AnyRef])
    metrics.put("getBatch", getBatch.asInstanceOf[AnyRef])
    metrics.put("inputRowsPerSecond", inRowsPerSecond.asInstanceOf[AnyRef])
    metrics.put("procRowsPerSecond", procRowsPerSecond.asInstanceOf[AnyRef])
    metrics.put("inputRows", inputRows.asInstanceOf[AnyRef])
    metrics.put("DateValue", date.toString())

    metrics

  }

  def parseError(e: Exception): HashMap[String, AnyRef] = {
    val date = java.time.format
      .DateTimeFormatter.RFC_1123_DATE_TIME.format(ZonedDateTime.now(ZoneId.of("GMT")))
    val error = new HashMap[String, AnyRef]()
    error.put("message", e.getLocalizedMessage)
    error.put("stack", e.getStackTrace)
    error.put("DateValue", date.toString())

    error

  }

  def parseRow(row: Row): HashMap[String, AnyRef] = {
    val date = java.time.format
      .DateTimeFormatter.RFC_1123_DATE_TIME.format(ZonedDateTime.now(ZoneId.of("GMT")))

    val bizmetrics = new HashMap[String, AnyRef]()


    for (i <- 0 until row.length - 1) {
      bizmetrics.put(row.schema.fields(i).name, row(i).asInstanceOf[AnyRef])
    }

    bizmetrics.put("DateValue", date.toString())


    bizmetrics

  }

}
