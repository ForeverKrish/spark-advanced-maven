package com.example.spark

import org.apache.spark.sql.SparkSession

object CatalystOptimiser {
  def main(args: Array[String]):Unit = {
    val spark = SparkSession.builder()
      .appName("Catalystptimizer Test")
      .master("local[*]").getOrCreate()
    import spark.implicits._

    val data = Seq(
      ("x",1),("y",2),("x",3)
    ).toDF("k","v")

    val processedData = data
      .filter("v > 1") //.filter($"v" > 1)
      .groupBy("k")
      .sum("v")

    // Extended explain: parsed, analyzed, optimized, physical
    processedData.explain(true)

    //filteredData.show()
    spark.stop()
  }
}
