package com.microsoft.pnp


import org.apache.log4j._
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener._
import org.json4s._

class StreamingMetricsListener() extends StreamingQueryListener {


  implicit val formats = DefaultFormats
  lazy val logger: Logger = Logger.getLogger("Log4jALALogger")


  override def onQueryStarted(event: QueryStartedEvent): Unit = {

  }

  override def onQueryProgress(event: QueryProgressEvent): Unit = {


    try {
      //parsing the telemetry Payload and logging to ala
      logger.info(Utils.parsePayload(event))
    }

    catch {
      case e: Exception => {
        //parsing the error payload and logging to ala
        logger.error(Utils.parseError(e))
      }
    }

  }

  override def onQueryTerminated(event: QueryTerminatedEvent): Unit = {

    if (event.exception.nonEmpty) {
      logger.error(event.exception)
    }


  }


}