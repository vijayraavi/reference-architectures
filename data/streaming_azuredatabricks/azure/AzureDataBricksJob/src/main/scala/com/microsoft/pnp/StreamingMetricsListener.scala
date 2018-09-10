package com.microsoft.pnp


import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}
import org.json4s.DefaultFormats
import org.slf4j.LoggerFactory


class StreamingMetricsListener() extends StreamingQueryListener {


  implicit val formats = DefaultFormats

  val logger = LoggerFactory.getLogger("StreamingMetricsListener")


  override def onQueryStarted(event: QueryStartedEvent): Unit = {


  }

  override def onQueryProgress(event: QueryProgressEvent): Unit = {


    try {
      //parsing the telemetry Payload and logging to ala
      logger.info("{}", Utils.parsePayload(event))
    }

    catch {
      case e: Exception => {
        logger.error("{}", Utils.parseError(e))
      }
    }

  }

  override def onQueryTerminated(event: QueryTerminatedEvent): Unit = {


  }


}