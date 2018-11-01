package org.apache.spark.metrics.source
import org.apache.spark.SparkContext
import org.apache.spark.util.LongAccumulator

object AppAccumulators {
  @volatile private var fareInstance: LongAccumulator = null
  @volatile private var rideInstance: LongAccumulator = null

  def getFareInstance(sc: SparkContext): LongAccumulator = {
    if (fareInstance == null) {
      synchronized {
        if (fareInstance == null) {
          fareInstance = sc.longAccumulator("FareCount")
        }
      }
    }
    fareInstance
  }

  def getRideInstance(sc: SparkContext): LongAccumulator = {
    if (rideInstance == null) {
      synchronized {
        if (rideInstance == null) {
          rideInstance = sc.longAccumulator("RideCount")
        }
      }
    }
    rideInstance
  }
}
