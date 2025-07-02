package com.example.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, to_date}
object HiveParquetLoaderSparkSQL{
  def main(args: Array[String]):Unit = {
    val spark = SparkSession.builder()
      .appName("HiveParquetLoader")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()
    // 1. Read Parquet and add event_date
    val rawData = spark.read.parquet("spark-advanced-maven/src/main/resources/parquet")
      .withColumn("event_date", to_date(col("event_time")))

    // 2. Register as a temp view
    rawData.createOrReplaceTempView("source_data")

    // 3. Use Spark SQL with ROW_NUMBER() to keep only the latest row per id
    val processedData = spark.sql(
      """SELECT
        id,
        event_time,
        event_date
        FROM
        ( SELECT
          *,
          ROW_NUMBER() OVER (PARTITION BY  id ORDER BY event_time DESC) as rn
        FROM source_data
        ) t
        WHERE t.rn = 1""")

    // 4. Now you can write `processedData` into your partitioned Hive table:
    processedData.write
      .mode("overwrite")
      .partitionBy("event_date")
      .insertInto("target_db.target_table")
    spark.stop()

  }
}
