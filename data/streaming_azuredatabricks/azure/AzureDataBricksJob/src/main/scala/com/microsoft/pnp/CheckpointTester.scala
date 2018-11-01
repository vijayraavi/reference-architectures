package com.microsoft.pnp

import org.apache.spark.eventhubs.{EventHubsConf, EventPosition}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{from_json, lit, when}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StringType

object CheckpointTester {

  def main(args: Array[String]) {

    val spark = SparkSession.builder().config("spark.master", "local[10]").getOrCreate()
    import spark.implicits._

    val sourceConnectionString = ""

    val options1 = EventHubsConf(sourceConnectionString)
      .setConsumerGroup("nithin")
      .setStartingPosition(EventPosition.fromStartOfStream)

//    val options2 = EventHubsConf(sourceConnectionString)
//      .setConsumerGroup("$Default")
//      .setStartingPosition(EventPosition.fromStartOfStream)

    val rides1 = spark.readStream
      .format("eventhubs")
      .options(options1.toMap)
      .load

//    val rides2 = spark.readStream
//      .format("eventhubs")
//      .options(options2.toMap)
//      .load

    val transformedRides1 = rides1
      .select(
        $"body"
          .cast(StringType)
          .as("messageData"),
        from_json($"body".cast(StringType), RideSchema)
          .as("ride"))
      .transform(ds => {
        ds.withColumn(
          "errorMessage",
          when($"ride".isNull,
            lit("Error decoding JSON"))
            .otherwise(lit(null))
        )
      })

//    val transformedRides2 = rides2
//      .select(
//        $"body"
//          .cast(StringType)
//          .as("messageData"),
//        from_json($"body".cast(StringType), RideSchema)
//          .as("ride"))
//      .transform(ds => {
//        ds.withColumn(
//          "errorMessage",
//          when($"ride".isNull,
//            lit("Error decoding JSON"))
//            .otherwise(lit(null))
//        )
//      })

    transformedRides1.printSchema()


    val actualRides1 = transformedRides1
      .filter(r => {
        if (r.isNullAt(r.fieldIndex("errorMessage"))) {
          true
        }
        else {
          false
        }
      })
      .select(
        $"ride.*"
      )
      .withWatermark("pickupTime", "3 minutes")

//    val actualRides2 = transformedRides2
//      .filter(r => {
//        if (r.isNullAt(r.fieldIndex("errorMessage"))) {
//          true
//        }
//        else {
//          false
//        }
//      })
//      .select(
//        $"ride.*"
//      )
//      .withWatermark("pickupTime", "3 minutes")


//    val rides_passenger_count_greater_2 = actualRides1.filter($"passengerCount" > 2)
//    val rides_passenger_count_lesser_2 = actualRides2.filter($"passengerCount" <= 2)



//    rides_passenger_count_greater_2.writeStream
//      .queryName("ridesWithPassengerCountMoreThanTwo")
//      .format("console")
//      .outputMode(OutputMode.Append())
//      .start()

//    rides_passenger_count_lesser_2.writeStream
//      .queryName("ridesWithPassengerCountLessThanOrEqualTwo")
//      .format("console")
//      .outputMode(OutputMode.Append())
//      .start()
//      .awaitTermination()


    actualRides1
      .writeStream
      .queryName("writeTokafka")
      .format("kafka")
      .outputMode(OutputMode.Append())
      .option("kafka.bootstrap.servers", "")
      .option("topic", "ride")
      .start()
      .awaitTermination()


  }

}
