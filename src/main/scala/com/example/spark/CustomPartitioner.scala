package com.example.spark

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object CustomPartitioner {
  def main(args: Array[String]):Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Custom Partitioner")
    val sc = new SparkContext(conf)
    //Simulate skewed data: many "C" keys
    val data = Seq(("A",1),("B",2)) ++ Seq.fill(1000)("C",3)
    val rdd = sc.parallelize(data)
    //rdd.foreach(println)
    // Partition by 4 partitions
    val currentDataBeforeSalting = rdd.partitionBy(new HashPartitioner(4))
    println("Partition sizes Before Salting:")
    currentDataBeforeSalting.mapPartitionsWithIndex({ case (i,iter) => Iterator(s"Part $i -> ${iter.size}")})
      .collect()
      .foreach(println)

    // Salt the “C” key into 4 sub-keys
    val saltedData = rdd.flatMap{ case(k,v) =>
      if (k.equals("C")) (0 until 4).map(i => (s"C#$i",v))
      else Seq((k,v))
    }

    // Partition by 4 partitions
    val partitionedData = saltedData.partitionBy(new HashPartitioner(4))
    println("Partition sizes after salting:")
    partitionedData.mapPartitionsWithIndex({ case (i,iter) => Iterator(s"Part $i -> ${iter.size}")})
      .collect()
      .foreach(println)

    // Remove salt and aggregate back
    val finalData = partitionedData.map{case (k,v)=>
      val key = k.split("#")(0); (key,v)
    }.reduceByKey(_+_)

    finalData.collect().foreach(println)
  }
}
