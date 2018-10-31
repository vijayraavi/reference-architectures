package com.microsoft.pnp

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, SparkSession}

object CassandraDriver {

//  val sc = SparkHelper.getSparkSession().sparkContext
//
//  println("Cassandra driver intialization")
//
//  for (elem <- sc.getConf.getAll) {
//
//    println(elem._1)
//    println(elem._2)
//  }

//  val connector = CassandraConnector(sc.getConf)
//  val namespace = "nsp"
//  val foreachTableSink = "test"

  def saveForeach(df: DataFrame, con:CassandraConnector) = {

    df
      .writeStream
      .queryName("eventHubtoCassandraInsert")
      .outputMode(OutputMode.Append())
      .foreach(new CassandraSinkForeach(con))
      .start()
      .awaitTermination()

  }


}
