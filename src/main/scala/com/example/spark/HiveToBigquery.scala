package com.example.spark

import org.apache.spark.sql.SparkSession

object HiveToBigquery {
  def main(args: Array[String]): Unit = {
    // 1. Build SparkSession with Hive support and BigQuery connector on the classpath
    val spark = SparkSession.builder()
      .appName("HiveToBigQuery")
      .enableHiveSupport()
      .getOrCreate()

    // 2. Read directly from your Hive table
    //    Replace "source_db.source_table" with your Hive database & table
    val hiveDF = spark.table("source_db.source_table")

    // 3. Write to BigQuery
    //    - table: destination in the form "project.dataset.table"
    //    - temporaryGcsBucket: a GCS bucket you own for shuffle files
    hiveDF.write
      .format("bigquery")
      .option("table", "my-gcp-project.my_dataset.my_bq_table")
      .option("temporaryGcsBucket", "my-temp-gcs-bucket")
      .mode("overwrite")  // or "append"
      .save()

    spark.stop()
  }
}
