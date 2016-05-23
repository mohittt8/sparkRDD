package com.mohit.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Test extends App {
  val conf = new SparkConf().setAppName("test").setMaster("local")
  val sc = new SparkContext(conf)

  val distFile = sc.textFile("test.txt")
  val q = distFile.filter(line => line.contains("scala123"))
  q.collect().foreach(println)
  //distFile.groupBy(identity).map{case (c, it) => (c, it.size)}.collect().foreach(println)
}