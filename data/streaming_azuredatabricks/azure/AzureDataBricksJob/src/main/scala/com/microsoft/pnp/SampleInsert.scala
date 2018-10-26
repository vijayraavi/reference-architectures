package com.microsoft.pnp

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.cassandra.CassandraFormat

object SampleInsert {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .config("spark.master", "local[10]")
      .config("spark.cassandra.output.batch.size.rows", "1")
      .config("spark.cassandra.connection.connections_per_executor_max", "10")
      .config("spark.cassandra.output.concurrent.writes", "1000")
      .config("spark.cassandra.concurrent.reads", "512")
      .config("spark.cassandra.output.batch.grouping.buffer.size", "1000")
      .config("spark.cassandra.connection.keep_alive_ms", "600000000").getOrCreate()
    import spark.implicits._


    // Generate a simple dataset containing five values
    val booksDF = Seq(
      ("b00001", "Arthur Conan Doyle", "A study in scarlet", 1887,11.33),
      ("b00023", "Arthur Conan Doyle", "A sign of four", 1890,22.45),
      ("b01001", "Arthur Conan Doyle", "The adventures of Sherlock Holmes", 1892,19.83),
      ("b00501", "Arthur Conan Doyle", "The memoirs of Sherlock Holmes", 1893,14.22),
      ("b00300", "Arthur Conan Doyle", "The hounds of Baskerville", 1901,12.25)
    ).toDF("book_id", "book_author", "book_name", "book_pub_year","book_price")

    booksDF.write
      .mode("append")
      .format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> "books", "keyspace" -> "books_ks", "output.consistency.level" -> "ALL", "ttl" -> "10000000"))
      .save()

  }
}
