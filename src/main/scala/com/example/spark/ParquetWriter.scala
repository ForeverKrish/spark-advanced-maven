package com.example.spark

import org.apache.spark.sql.{SaveMode, SparkSession}

object ParquetWriter {
  def main(args: Array[String]): Unit = {
    // Usage: ParquetWriterExample <outputPath>
    if (args.length != 1) {
      System.err.println("Usage: ParquetWriterExample <outputPath>")
      System.exit(1)
    }
    val outputPath = args(0)

    // 1. Initialize SparkSession
    val spark = SparkSession.builder()
      .appName("ParquetWriterExample")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._

    // 2. Create a sample DataFrame
    val data = Seq(
      (1, "Alice", 34),
      (2, "Bob",   28),
      (3, "Charlie", 45),
      (4, "David", 23),
      (5, "Eve",   37)
    ).toDF("id", "name", "age")

    // 3. Write out as Parquet
    data.write
      .mode(SaveMode.Overwrite)             // overwrite existing data
      .option("compression", "snappy")      // use snappy compression
      .partitionBy("age")                   // partition files by age
      .parquet(s"$outputPath/people.parquet")
    spark.stop()
  }
}
