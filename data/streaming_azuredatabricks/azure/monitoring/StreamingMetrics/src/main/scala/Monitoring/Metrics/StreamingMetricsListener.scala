package Monitoring.Metrics

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
      logger.info(parsePayload(event))
    }

    catch {
      case e: Exception => {
        //parsing the error payload and logging to ala
        logger = Logger.getLogger("Log4jALAError")
        logger.error(parseError(e))
      }
    }

  }

  override def onQueryTerminated(event: QueryTerminatedEvent): Unit = {

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

  def parsePayload(event: QueryProgressEvent): HashMap[String, AnyRef]={
    val date = java.time.format
      .DateTimeFormatter.RFC_1123_DATE_TIME.format(ZonedDateTime.now(ZoneId.of("GMT")))

    var dataJson = parse(event.progress.json).asInstanceOf[JObject]
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

    val metrics = new HashMap[String, AnyRef]()

    metrics.put("name",event.progress.name)
    metrics.put("id",  id)
    metrics.put("sink",  sink)
    metrics.put("triggerExecution",  triggerExecution.asInstanceOf[AnyRef])
    metrics.put("getBatch", getBatch.asInstanceOf[AnyRef])
    metrics.put("inputRowsPerSecond",  inRowsPerSecond.asInstanceOf[AnyRef])
    metrics.put("procRowsPerSecond",  procRowsPerSecond.asInstanceOf[AnyRef])
    metrics.put("inputRows",  inputRows.asInstanceOf[AnyRef])
    metrics.put("DateValue",date.toString())

    metrics



  }

}
