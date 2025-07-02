package com.example.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number}

object HiveParquetLoader{
  def main(args: Array[String]):Unit = {
    val spark = SparkSession.builder()
      .appName("HiveParquetLoader")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    val rawData = spark.read.parquet("spark-advanced-maven/src/main/resources/parquet")

    val windowSpec = Window.partitionBy("id").orderBy(col("event_time").desc)
    val processedData = rawData
      .withColumn("rnk",row_number().over(windowSpec))
      .filter(col("rnk") === 1)
      .drop("rnk")

    processedData.write.mode("overwrite").insertInto("targetTable")

    spark.stop()

  }
}
