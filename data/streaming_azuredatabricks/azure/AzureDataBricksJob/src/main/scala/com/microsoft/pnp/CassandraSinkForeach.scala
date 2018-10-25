//package com.microsoft.pnp
//
//import org.apache.spark.sql.ForeachWriter
//
//class CassandraSinkForeach() extends ForeachWriter[MockObject] {
//
//  private def getInsertStatement(mockObject: MockObject): String =
//    s"""
//       |insert into ${CassandraDriver.namespace}.${CassandraDriver.foreachTableSink} (field1, field2)
//       |       values('${mockObject.field1}', '${mockObject.field2}')"""
//
//
//  override def open(partitionId: Long, version: Long): Boolean = {
//
//    true
//  }
//
//  override def process(mockObject: MockObject): Unit = {
//
//    CassandraDriver.connector.withSessionDo(session => session.execute(getInsertStatement(mockObject)))
//
//  }
//
//  override def close(errorOrNull: Throwable): Unit = {
//
//  }
//}
