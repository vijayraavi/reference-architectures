package monitoring.com.microsoft.pnp

import org.apache.log4j._
import org.apache.spark.streaming.scheduler.StreamingListener
import org.apache.spark.streaming.scheduler._

import org.json4s._


class BatchMetricsListener() extends StreamingListener {


  implicit val formats = DefaultFormats
  lazy val logger:Logger = Logger.getLogger("Log4jALALogger")

  override def onStreamingStarted(streamingStarted: StreamingListenerStreamingStarted): Unit = {


  }

  override def onReceiverStarted(receiverStarted: StreamingListenerReceiverStarted): Unit = {


  }

  override def onReceiverError(receiverError: StreamingListenerReceiverError): Unit = {


  }

  override def onReceiverStopped(receiverStopped: StreamingListenerReceiverStopped): Unit = {

  }

  override def onBatchSubmitted(batchSubmitted: StreamingListenerBatchSubmitted): Unit = {

  }

  override def onBatchStarted(batchStarted: StreamingListenerBatchStarted): Unit = {

  }

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {

  }

  override def onOutputOperationStarted(outputOperationStarted: StreamingListenerOutputOperationStarted): Unit = {

  }

  override def onOutputOperationCompleted(outputOperationCompleted: StreamingListenerOutputOperationCompleted): Unit = {


  }









}
