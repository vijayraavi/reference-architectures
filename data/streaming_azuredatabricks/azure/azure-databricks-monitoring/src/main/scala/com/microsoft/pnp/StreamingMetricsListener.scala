package com.microsoft.pnp

import org.apache.log4j.Logger
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener._

class StreamingMetricsListener() extends StreamingQueryListener {
  val logger: Logger = Logger.getLogger("Log4jALALogger")

  override def onQueryStarted(event: QueryStartedEvent): Unit = {}

  override def onQueryProgress(event: QueryProgressEvent): Unit = {
    try {
      //parsing the telemetry Payload and logging to ala
      this.logger.info(Utils.parsePayload(event))
    }

    catch {
      case e: Exception => {
        //parsing the error payload and logging to ala
        this.logger.error(Utils.parseError(e))
      }
    }
  }

  override def onQueryTerminated(event: QueryTerminatedEvent): Unit = {}
}
