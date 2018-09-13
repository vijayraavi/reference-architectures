package com.microsoft.pnp

import org.apache.spark.sql._
import org.apache.log4j._

class BusinessMetricsWriter extends ForeachWriter[Row] {
  @transient val logger = Logger.getLogger("Log4jALABizLogger")

  def open(partitionId: Long, version: Long): Boolean = {
    true
  }

  def process(value: Row): Unit = {
    try {
      //parsing the row fields / values as business telemetry
      this.logger.info(Utils.parseRow(value))
    }
    catch {
      case e: Exception => {
        //parsing the error payload and logging to ala
        this.logger.error(Utils.parseError(e))
      }
    }
  }

  def close(errorOrNull: Throwable): Unit = { }
}
