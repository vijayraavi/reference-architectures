package org.apache.spark.metrics.source
import org.apache.spark.SparkContext
import org.apache.spark.util.LongAccumulator

object AppAccumulators {
  @volatile private var processedInputCountInstance: LongAccumulator = null

  def getProcessedInputCountInstance(sc: SparkContext): LongAccumulator = {
    if (processedInputCountInstance == null) {
      synchronized {
        if (processedInputCountInstance == null) {
          processedInputCountInstance = sc.longAccumulator("ProcessedInputCount")
        }
      }
    }
    processedInputCountInstance
  }
}
