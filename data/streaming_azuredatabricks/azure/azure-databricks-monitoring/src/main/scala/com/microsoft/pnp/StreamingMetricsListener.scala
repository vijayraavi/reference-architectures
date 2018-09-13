package com.microsoft.pnp

import org.apache.log4j._
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener._
import org.json4s._
import org.json4s.native._
import org.json4s.native.JsonMethods._
import com.google.gson.Gson
import com.github.ptvlogistics.log4jala._
import java.util.HashMap
import java.time.ZoneId
import java.time.ZonedDateTime


class StreamingMetricsListener() extends StreamingQueryListener {


  implicit val formats = DefaultFormats
  var logger:Logger = Logger.getLogger("Log4jALALogger")

  override def onQueryStarted(event: QueryStartedEvent): Unit = {


  }

  override def onQueryProgress(event: QueryProgressEvent): Unit = {


    try {
      logger = Logger.getLogger("Log4jALALogger")
      //parsing the telemetry Payload and logging to ala
      logger.info(Utils.parsePayload(event))
    }

    catch {
      case e: Exception => {
        //parsing the error payload and logging to ala
        logger = Logger.getLogger("Log4jALAError")
        logger.error(Utils.parseError(e))
      }
    }

  }

  override def onQueryTerminated(event: QueryTerminatedEvent): Unit = {


  }



}
