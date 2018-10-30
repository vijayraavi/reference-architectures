package com.microsoft.pnp

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.SparkSession

class CassandraDriver {

  val sc = SparkSession.builder().getOrCreate().sparkContext

  sc.getConf.getAll.foreach(x => System.out.println(x))

  val connector = CassandraConnector(sc.getConf)
  val namespace = "nsp"
  val foreachTableSink = "test"


}
