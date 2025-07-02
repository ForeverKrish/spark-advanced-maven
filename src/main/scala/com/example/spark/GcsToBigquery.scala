import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object GcsToBigquery {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("GcsToBigQueryDedup")
      .getOrCreate()
    // 1. Read from GCS (assume Parquet with columns: id, event_time, ... )
    val gcsPath = "gs://my-gcs-bucket/path/to/data.parquet"
    val df = spark.read
      .format("parquet")
      .load(gcsPath)

    // 2. Define a window partitioned by 'id' and ordered by latest 'event_time'
    val windowSpec = Window
      .partitionBy("id")
      .orderBy(col("event_time").desc)

    // 3. Assign a row number per 'id' and filter for the first (latest) record
    val deduped = df
      .withColumn("rn", row_number().over(windowSpec))
      .filter(col("rn") === 1)
      .drop("rn")

    // 4. Write deduplicated DataFrame into BigQuery
    deduped.write
      .format("bigquery")
      .option("table", "my-gcp-project.my_dataset.my_bq_table")
      .option("temporaryGcsBucket", "my-temp-gcs-bucket")
      .mode("overwrite")  // or "append"
      .save()
    spark.stop()
  }
}
