package com.microsoft.pnp

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.scalatest.Matchers


class UtilsTests[sql] extends SparkSuiteBase with Matchers {

  test("should_parse_row_telemetry") {

    val schema = StructType(
      StructField("col1", StringType) ::
        StructField("col2", StringType) ::
        StructField("col3", IntegerType) :: Nil)
    val values = Array("value1", "value2", 1)
    val row = new GenericRowWithSchema(values, schema)

    val metrics = Utils.parseRow(row)

    assert(metrics.get("col1") === "value1")
    assert(metrics.get("col2") === "value2")
    assert(metrics.get("col3") === 1)
  }
}