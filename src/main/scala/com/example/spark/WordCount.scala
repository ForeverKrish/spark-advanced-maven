package com.example.spark

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

import java.net.URI

object WordCount {
  def main(args: Array[String]): Unit = {
    //RDD Wordcount
    val conf = new SparkConf()
      .setAppName("WordCount")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)
    val data = sc.textFile("spark-advanced-maven/src/main/resources/sample_input.txt")
    val wordCountDF = data
      .flatMap(line => line.split("\\s+"))
      .map(word => (word,1))
      .reduceByKey((val1,val2)=>val1+val2)
      .sortBy({ case (_,cnt)=>cnt}, ascending = false) //.sortByKey(ascending = true)

    //print
    wordCountDF.foreach(println)

    //save
    val outputPath = "spark-advanced-maven/src/main/resources/output/wc-out.txt"
    val path = new Path(outputPath)
    val fs = FileSystem.get(new URI(outputPath),sc.hadoopConfiguration)
    // delete if exists (recursive = true)
    if (fs.exists(path)) {
      fs.delete(path, true)
    }
    // now save
    wordCountDF
      .coalesce(1)
      .saveAsTextFile("spark-advanced-maven/src/main/resources/output/wc-out.txt")

    sc.stop()
  }
}
