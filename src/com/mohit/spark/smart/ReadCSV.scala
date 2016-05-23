package com.mohit.spark.smart

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

object ReadCSV extends App {

  val conf = new SparkConf().setAppName("test").setMaster("local")
  val sparkContext = new SparkContext(conf)
  val sc = new SQLContext(sparkContext)

  val df = sc.read
    .format("com.databricks.spark.csv")
    .option("header", "true") // Use first line of all files as header
    .option("inferSchema", "true") // Automatically infer data types
    .load("https://s3-us-west-2.amazonaws.com/bxbd-oregon/Events.csv")

  df.printSchema()

//  val total = df.count()
//
//  val distinct = df.distinct().count()

  val distinctCycleIds = 1
  
  println(df.select(countDistinct("cycleID")).head().getLong(0))
  
   // println(total + " " + distinct + " " + distinctCycleIds)

}