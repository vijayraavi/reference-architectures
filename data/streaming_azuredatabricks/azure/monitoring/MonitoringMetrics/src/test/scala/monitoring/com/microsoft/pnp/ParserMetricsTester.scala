package org.apache.spark.sql.streaming

import java.util.HashMap

import org.apache.spark.sql.types._

import monitoring.com.microsoft.pnp._
import org.scalatest.Matchers
import org.apache.spark.sql.streaming.StreamingQueryListener.QueryProgressEvent
import org.apache.spark.sql._
import scala.util.{Failure, Success, Try}
import java.util.UUID.randomUUID

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

class ParserMetricsTester extends SparkSuiteBase with Matchers {


  test("should_parse_queryprogress_telemetry") {
    val guid = randomUUID()
    val duration:java.util.Map[String,java.lang.Long] =  new HashMap[String, java.lang.Long]
    val eventTime:java.util.Map[String,String] =  new HashMap[String, String]

    duration.put("addBatch",100)
    duration.put("getBatch",200)
    val source:SourceProgress = new SourceProgress("source","start","end",100,200,300)
    val sourcearr = new Array[SourceProgress](1)
    sourcearr(0)=source

    val progressEvent = new QueryProgressEvent(
      new StreamingQueryProgress(
        guid,guid,
        "streamTest","time",
        10,duration,
        eventTime,
        null,sourcearr,null
    )
    )

    val metrics = Utils.parsePayload(progressEvent)
    assert(progressEvent.progress.id === metrics.get("id"))
    assert(progressEvent.progress.numInputRows === metrics.get("inputRows"))
    assert(progressEvent.progress.processedRowsPerSecond === metrics.get("procRowsPerSecond"))
    assert(progressEvent.progress.inputRowsPerSecond === metrics.get("inputRowsPerSecond"))
    assert(progressEvent.progress.durationMs.get("addBatch") ===
      metrics.get("durationms").asInstanceOf[HashMap[String, AnyRef]].get("addBatch"))
    assert(progressEvent.progress.durationMs.get("getBatch") ===
      metrics.get("durationms").asInstanceOf[HashMap[String, AnyRef]].get("getBatch"))

  }

  test("should_parse_exception_telemetry") {
    val exception = new Exception("error")
    val metrics = Utils.parseError(exception)
    assert(metrics.get("message") === "error")
  }

  test("should_parse_row_telemetry") {

    val schema = StructType(
      StructField("col1", StringType) ::
        StructField("col2", StringType) ::
        StructField("col3", IntegerType) :: Nil)
    val values = Array("value1", "value2", 1)
  //  val values = Array("namevalue", 10)
    val row = new GenericRowWithSchema(values , schema)

    val metrics = Utils.parseRow(row)

    assert(metrics.get("col1") === "value1")
    assert(metrics.get("col2") === "value2")
    assert(metrics.get("col3") === 1)

  }




}
