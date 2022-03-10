package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test {

  def main(args: Array[String]): Unit = {

    println("Hello Spark")

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
    val sparkContext = new SparkContext(sparkConf)
    val dataRDD: RDD[Int] = sparkContext.makeRDD(
      List(1, 2, 3, 4),
      4)
    val fileRDD: RDD[String] = sparkContext.textFile(
      "input", 2)
    fileRDD.collect().foreach(println)
    sparkContext.stop()


  }
}
