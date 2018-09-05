package monitoring.com.microsoft.pnp

import java.time.{ZoneId, ZonedDateTime}
import java.util.HashMap

import org.apache.spark.sql.streaming.StreamingQueryListener.QueryProgressEvent

import org.apache.spark._

import org.apache.spark.streaming._

import org.apache.spark.sql._
import org.apache.spark.sql.functions.get_json_object

import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming._
import com.google.gson.Gson;
import org.apache.spark.sql._
import com.github.ptvlogistics.log4jala._
import org.apache.log4j._


object Utils {


  def parsePayload(event: QueryProgressEvent): HashMap[String, AnyRef]={
    val date = java.time.format
      .DateTimeFormatter.RFC_1123_DATE_TIME.format(ZonedDateTime.now(ZoneId.of("GMT")))

    val metrics = new HashMap[String, AnyRef]()

    metrics.put("id",  event.progress.id)
    metrics.put("sink", event.progress.sink)
    metrics.put("durationms",
      event.progress.durationMs.asInstanceOf[AnyRef])
    metrics.put("inputRowsPerSecond",  event.progress.inputRowsPerSecond.asInstanceOf[AnyRef])
    metrics.put("procRowsPerSecond",  event.progress.processedRowsPerSecond.asInstanceOf[AnyRef])
    metrics.put("inputRows",  event.progress.numInputRows.asInstanceOf[AnyRef])
    metrics.put("DateValue",date.toString())


    metrics

  }

  def parseError(e:Exception) : HashMap[String,AnyRef] ={
    val date = java.time.format
      .DateTimeFormatter.RFC_1123_DATE_TIME.format(ZonedDateTime.now(ZoneId.of("GMT")))
    val error = new HashMap[String, AnyRef]()
    error.put("message",e.getLocalizedMessage)
    error.put("stack",  e.getStackTrace)
    error.put("DateValue",date.toString())

    error

  }

  def parseRow(row: Row) : HashMap[String,AnyRef] ={
    val date = java.time.format
      .DateTimeFormatter.RFC_1123_DATE_TIME.format(ZonedDateTime.now(ZoneId.of("GMT")))

    val bizmetrics = new HashMap[String, AnyRef]()



    for (i <- 0 until row.length-1) {
      bizmetrics.put(row.schema.fields(i).name,row(i).asInstanceOf[AnyRef])
    }

    bizmetrics.put("DateValue",date.toString())


    bizmetrics

  }

}
