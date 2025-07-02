package com.example.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
object KafkaToBigQueryStreaming {
  def main(args: Array[String]): Unit = {
    // 1. Create SparkSession (no local master here; pick up cluster mode from spark-submit)
    val spark = SparkSession.builder()
      .appName("KafkaToBigQueryStreaming")
      .getOrCreate()
    import spark.implicits._
    // 2. Define the schema of the JSON payload in Kafka “value”
    val payloadSchema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("event_time", TimestampType, nullable = false),
      StructField("some_metric", DoubleType, nullable = true)
      // add other fields as needed
    ))
    // 3. Read from Kafka as a streaming source
    val kafkaDf = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "broker1:9092,broker2:9092")
      .option("subscribe", "my_topic")
      .option("startingOffsets", "latest")
      .load()
      .selectExpr( // key and value come in as binary by default
        "CAST(key AS STRING)   AS kafka_key",
        "CAST(value AS STRING) AS json_str",
        "timestamp              AS kafka_timestamp"
      )
    // 4. Parse the JSON payload
    val parsed = kafkaDf
      .select(from_json($"json_str", payloadSchema).alias("data"))
      .select("data.*")  // now have columns id, event_time, some_metric, …
    // 5. (Optional) Deduplicate per micro-batch by id keeping latest event_time
    import org.apache.spark.sql.expressions.Window
    val windowSpec = Window.partitionBy("id").orderBy($"event_time".desc)
    val deduped = parsed
      .withColumn("rn", row_number().over(windowSpec))
      .filter($"rn" === 1)
      .drop("rn")
    // 6. Write the stream into BigQuery, checkpointing offsets in GCS
    val query = deduped.writeStream
      .format("bigquery")
      .option("table", "my-gcp-project.my_dataset.my_bq_table")
      .option("temporaryGcsBucket", "my-temp-gcs-bucket")
      .option("checkpointLocation", "gs://my-temp-gcs-bucket/checkpoints/kafka-to-bq")
      .outputMode("append")
      .start()
    // 7. Await termination
    query.awaitTermination()
  }
}/*
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --jars spark-bigquery-with-dependencies_2.12-0.30.0.jar \
  --class KafkaToBigQueryStreaming \
  /path/to/your-assembly.jar
MERGE INTO `your_project.your_dataset.final_table` AS T
USING (
  -- Deduplicate raw on id, keeping the most recent event_time
  SELECT *
  FROM `your_project.your_dataset.raw_table`
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY id
      ORDER BY event_time DESC
    ) = 1
) AS S
ON T.id = S.id

WHEN MATCHED THEN
  UPDATE SET
    T.event_time = S.event_time,
    T.col1       = S.col1,
    T.col2       = S.col2
    -- …add other columns as needed
WHEN NOT MATCHED THEN
  INSERT ROW
;
* */