package com.example.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode, split, window}
import org.apache.spark.sql.streaming.Trigger
object Streaming {
  def main(args: Array[String]): Unit = {
    if (args.length != 3) {System.exit(1)}
    val Array(bootstrapServers, topic, checkpointDir) = args
    val spark = SparkSession.builder()
      .appName("KafkaStructuredStreaming")
      .master("local[*]").getOrCreate()
    // Read raw Kafka stream (schema: key, value, topic, partition, offset, timestamp, timestampType)
    val kafkaDF = spark.readStream
      .format("kafka").option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", topic).option("startingOffsets", "earliest").load()
    // Project only the fields we need: cast value→string, keep timestamp
    val messagesDF = kafkaDF
      .select(
        col("timestamp"),
        col("value").cast("string").as("message"))
    // Explode into one row per word, keep the same timestamp
    val wordsDF = messagesDF
      .withColumn("word", explode(split(col("message"), "\\s+")))
      .select(col("word"), col("timestamp"))
    // Windowed aggregation on event time with watermark
    val countsDF = wordsDF
      .withWatermark("timestamp", "30 seconds")
      .groupBy(
        window(col("timestamp"), "10 seconds", "5 seconds"),
        col("word")
      ).count()
    // Write to console
    val query = countsDF.writeStream
      .outputMode("append").format("console")
      .option("truncate", false).option("checkpointLocation", checkpointDir)
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()
    query.awaitTermination()
  }
}
