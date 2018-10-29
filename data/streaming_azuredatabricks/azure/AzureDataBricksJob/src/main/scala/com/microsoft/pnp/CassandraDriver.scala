package com.microsoft.pnp

import com.datastax.spark.connector.cql.CassandraConnector

class CassandraDriver {

  val spark = SparkHelper.getSparkSession()

  import spark.implicits._

  val connector = CassandraConnector(spark.sparkContext.getConf)

  val namespace = "newyorktaxi"
  val foreachTableSink = "neighborhooddata"


}
