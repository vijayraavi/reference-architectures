package com.microsoft.pnp

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.ForeachWriter

class CassandraSinkForeach(con:CassandraConnector) extends ForeachWriter[org.apache.spark.sql.Row] {
  // This class implements the interface ForeachWriter, which has methods that get called
  // whenever there is a sequence of rows generated as output


  def open(partitionId: Long, version: Long): Boolean = {
    // open connection
    println(s"Open connection")
    true
  }

  def process(record: org.apache.spark.sql.Row) = {
    println(s"Process new $record")

    con.withSessionDo(session =>
      session.execute(
        //        s"""
        //       insert into ${cassandraDriver.namespace}.${cassandraDriver.foreachTableSink} (pickupneighborhood, windowstart,windowend,ridecount,totalfareamount,totaltipamount)
        //       values('${record(0)}', '${record(1)}', '${record(2)}', '${record(3)}', '${record(4)}', '${record(5)}')"""

        s"""
           |insert into nsp.test (field1, field2)
           |       values('${record(0)}', '${record(1)}')"""

      )
    )
  }

  def close(errorOrNull: Throwable): Unit = {
    // close the connection
    println(s"Close connection")
  }
}
