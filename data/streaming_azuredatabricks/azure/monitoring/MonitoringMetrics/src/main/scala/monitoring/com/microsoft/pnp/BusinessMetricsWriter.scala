package monitoring.com.microsoft.pnp

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

class BusinessMetricsWriter extends ForeachWriter[Row] {
  @transient lazy val logger = Logger.getLogger("Log4jALABizLogger")

  def open(partitionId: Long,version: Long): Boolean = {

    true
  }

  def process(value: Row): Unit = {

    try {
      //parsing the row fields / values as business telemetry
      logger.info(Utils.parseRow(value))
    }
    catch {
      case e: Exception => {
        //parsing the error payload and logging to ala
        logger.error(Utils.parseError(e))
      }
    }
  }

  def close(errorOrNull: Throwable): Unit = {
  }

}
