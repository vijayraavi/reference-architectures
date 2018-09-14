package org.apache.spark.metrics.source

import com.codahale.metrics.{Gauge, MetricRegistry}
import org.apache.spark.SparkContext
import org.apache.spark.util.LongAccumulator

class AppMetrics(sc: SparkContext) extends Source {
  override val metricRegistry = new MetricRegistry
  override val sourceName = "%s.AppMetrics".format(sc.appName)

//  def registerGauge(metricName: String, value: Long) {
//    val metric = new Gauge[Long] {
//      override def getValue: Long = {
//        value
//      }
//    }
//
//    metricRegistry.register(MetricRegistry.name(metricName), metric)
//  }

  //def registerGauge(metricName: String, acc: Accumulator[Long]) {
  def registerGauge(metricName: String, acc: LongAccumulator) {
    val metric = new Gauge[Long] {
      override def getValue: Long = {
        acc.value
      }
    }

    metricRegistry.register(MetricRegistry.name(metricName), metric)
  }
}

object StaticAppMetrics extends Source {
  override val sourceName: String = "StaticAppMetrics"
  override val metricRegistry: MetricRegistry = new MetricRegistry()

  val METRIC_X_VALUE = metricRegistry.histogram(MetricRegistry.name("xValue"))

  val METRIC_Y_VALUE = metricRegistry.histogram(MetricRegistry.name("yValue"))
}

