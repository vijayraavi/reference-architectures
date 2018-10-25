package com.microsoft.pnp

//import com.datastax.spark.connector.cql.CassandraConnector
//import org.apache.spark.sql.DataFrame
//
//object CassandraDriver {
//
//  val spark = SparkHelper.getSparkSession()
//
//  import spark.implicits._
//
//  val connector = CassandraConnector(SparkHelper.getSparkSession().sparkContext.getConf)
//
//  val namespace = "nsp"
//
//  val foreachTableSink = "test"
//
//  def saveForeach(df: DataFrame) = {
//
//    df.as[MockObject]
//      .writeStream
//      .queryName("eventHubtoCassandraInsert")
//      .outputMode("update")
//      .foreach(new CassandraSinkForeach())
//      .start()
//      .awaitTermination()
//
//  }
//}
