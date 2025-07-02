package com.example.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.broadcast

object BroadCastJoin {
  def main(args: Array[String]):Unit = {
    val spark = SparkSession.builder()
      .appName("Performance Tuning")
      .config("spark.sql.autoBroadcastJoinThreshold", 5*1024*1024)
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    // 1. Caching a large range
    val largeDF = spark.range(1,10000000).toDF("id").cache()
    largeDF.count()  // materialize cache

    // 2. Broadcast join with small lookup
    val lookup = Seq((1,"one"),(2,"two")).toDF("id","name")
    val joined = largeDF.join(broadcast(lookup),"id")
    joined.show(5)

    spark.stop()
  }
}
