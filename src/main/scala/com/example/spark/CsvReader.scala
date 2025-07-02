package com.example.spark

import org.apache.spark.sql.SparkSession

object CsvReader {
 def main(args: Array[String]): Unit = {
   val spark = SparkSession.builder()
     .master("local[*]")
     .appName("CsvReader")
     .getOrCreate()

   val csvData = spark.read
     .option("header","true")
     .option("inferSchema","true")
     .csv("src/main/resources/sample_data.csv")

   csvData.printSchema()
   csvData.show(numRows = 10,truncate = false)//csvData.show(10,false)
   spark.stop()
 }
}
