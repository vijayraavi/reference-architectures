package com.microsoft.pnp

import com.microsoft.pnp.slf4j.MDCCloseableFactory
import com.microsoft.pnp.spark.TryWith
import org.apache.spark.sql._
import org.slf4j.{Logger, LoggerFactory}

class BusinessMetricsWriter extends ForeachWriter[Row] {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(this.getClass.getName.stripSuffix("$"))
  lazy val mdcFactory: MDCCloseableFactory = new MDCCloseableFactory()

  def open(partitionId: Long, version: Long): Boolean = {
    true
  }

  def process(value: Row): Unit = {
    try {
      //parsing the row fields / values as business telemetry
      TryWith(this.mdcFactory.create(Utils.parseRow(value)))(
        c => {
          this.logger.info("process")
        }
      )
    }
    catch {
      case e: Exception => {
        this.logger.error("process", e)
      }
    }
  }

  def close(errorOrNull: Throwable): Unit = { }
}
