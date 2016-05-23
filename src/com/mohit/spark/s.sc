package com.mohit.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext

object s {
  val conf = new SparkConf().setAppName("test").setMaster("local")
                                                  //> conf  : org.apache.spark.SparkConf = org.apache.spark.SparkConf@1f26261c
  val sparkContext = new SparkContext(conf)       //> Using Spark's default log4j profile: org/apache/spark/log4j-defaults.propert
                                                  //| ies
                                                  //| 16/05/20 19:01:31 INFO SparkContext: Running Spark version 1.6.1
                                                  //| 16/05/20 19:01:32 WARN NativeCodeLoader: Unable to load native-hadoop librar
                                                  //| y for your platform... using builtin-java classes where applicable
                                                  //| 16/05/20 19:01:33 WARN Utils: Your hostname, mohit resolves to a loopback ad
                                                  //| dress: 127.0.1.1; using 172.17.0.1 instead (on interface docker0)
                                                  //| 16/05/20 19:01:33 WARN Utils: Set SPARK_LOCAL_IP if you need to bind to anot
                                                  //| her address
                                                  //| 16/05/20 19:01:33 INFO SecurityManager: Changing view acls to: mohit
                                                  //| 16/05/20 19:01:33 INFO SecurityManager: Changing modify acls to: mohit
                                                  //| 16/05/20 19:01:33 INFO SecurityManager: SecurityManager: authentication disa
                                                  //| bled; ui acls disabled; users with view permissions: Set(mohit); users with 
                                                  //| modify permissions: Set(mohit)
                                                  //| 16/05/20 19:01:34 INFO Utils: Successfully started service 'sparkDriver' on 
                                                  //| port 42131.
  val sc = new SQLContext(sparkContext)           //> sc  : org.apache.spark.sql.SQLContext = org.apache.spark.sql.SQLContext@5cf9
                                                  //| 0a8b

  val df = sc.read
    .format("com.databricks.spark.csv")
    .option("header", "true") // Use first line of all files as header
    .option("inferSchema", "true") // Automatically infer data types
    .load("/home/mohit/smarttrack/locations/Events.csv")
                                                  //> 16/05/20 19:01:37 INFO MemoryStore: Block broadcast_0 stored as values in me
                                                  //| mory (estimated size 104.0 KB, free 104.0 KB)
                                                  //| 16/05/20 19:01:37 INFO MemoryStore: Block broadcast_0_piece0 stored as bytes
                                                  //|  in memory (estimated size 9.8 KB, free 113.8 KB)
                                                  //| 16/05/20 19:01:37 INFO BlockManagerInfo: Added broadcast_0_piece0 in memory 
                                                  //| on localhost:42662 (size: 9.8 KB, free: 1060.5 MB)
                                                  //| 16/05/20 19:01:37 INFO SparkContext: Created broadcast 0 from textFile at Te
                                                  //| xtFile.scala:30
                                                  //| 16/05/20 19:01:38 INFO FileInputFormat: Total input paths to process : 1
                                                  //| 16/05/20 19:01:38 INFO SparkContext: Starting job: first at CsvRelation.scal
                                                  //| a:267
                                                  //| 16/05/20 19:01:38 INFO DAGScheduler: Got job 0 (first at CsvRelation.scala:2
                                                  //| 67) with 1 output partitions
                                                  //| 16/05/20 19:01:38 INFO DAGScheduler: Final stage: ResultStage 0 (first at Cs
                                                  //| vRelation.scala:267)
                                                  //| 16/05/20 19:01:38 INFO DAGScheduler: Parents of final stage: List()
                                                  //| 16/05/20 19:01:38 INFO DAGSchedul
                                                  //| Output exceeds cutoff limit.

  println("totlal " + df.count())                 //> 16/05/20 19:04:13 INFO MemoryStore: Block broadcast_4 stored as values in me
                                                  //| mory (estimated size 144.6 KB, free 233.9 KB)
                                                  //| 16/05/20 19:04:13 INFO MemoryStore: Block broadcast_4_piece0 stored as bytes
                                                  //|  in memory (estimated size 13.8 KB, free 247.7 KB)
                                                  //| 16/05/20 19:04:13 INFO BlockManagerInfo: Added broadcast_4_piece0 in memory 
                                                  //| on localhost:42662 (size: 13.8 KB, free: 1060.5 MB)
                                                  //| 16/05/20 19:04:13 INFO SparkContext: Created broadcast 4 from textFile at Te
                                                  //| xtFile.scala:30
                                                  //| 16/05/20 19:04:14 INFO FileInputFormat: Total input paths to process : 1
                                                  //| 16/05/20 19:04:14 INFO SparkContext: Starting job: count at com.mohit.spark.
                                                  //| s.scala:18
                                                  //| 16/05/20 19:04:14 INFO DAGScheduler: Registering RDD 12 (count at com.mohit.
                                                  //| spark.s.scala:18)
                                                  //| 16/05/20 19:04:14 INFO DAGScheduler: Got job 2 (count at com.mohit.spark.s.s
                                                  //| cala:18) with 1 output partitions
                                                  //| 16/05/20 19:04:14 INFO DAGScheduler: Final stage: ResultStage 3 (count at co
                                                  //| m.mohit.spark.s
                                                  //| Output exceeds cutoff limit.

  println("distinct " + df.distinct().count())    //> 16/05/20 19:06:07 INFO MemoryStore: Block broadcast_7 stored as values in me
                                                  //| mory (estimated size 144.6 KB, free 355.8 KB)
                                                  //| 16/05/20 19:06:07 INFO MemoryStore: Block broadcast_7_piece0 stored as bytes
                                                  //|  in memory (estimated size 13.8 KB, free 369.6 KB)
                                                  //| 16/05/20 19:06:07 INFO BlockManagerInfo: Added broadcast_7_piece0 in memory 
                                                  //| on localhost:42662 (size: 13.8 KB, free: 1060.5 MB)
                                                  //| 16/05/20 19:06:07 INFO SparkContext: Created broadcast 7 from textFile at Te
                                                  //| xtFile.scala:30
                                                  //| 16/05/20 19:06:08 INFO FileInputFormat: Total input paths to process : 1
                                                  //| 16/05/20 19:06:08 INFO SparkContext: Starting job: count at com.mohit.spark.
                                                  //| s.scala:20
                                                  //| 16/05/20 19:06:08 INFO DAGScheduler: Registering RDD 22 (count at com.mohit.
                                                  //| spark.s.scala:20)
                                                  //| 16/05/20 19:06:08 INFO DAGScheduler: Registering RDD 26 (count at com.mohit.
                                                  //| spark.s.scala:20)
                                                  //| 16/05/20 19:06:08 INFO DAGScheduler: Got job 3 (count at com.mohit.spark.s.s
                                                  //| cala:20) with 1 output partitio
                                                  //| Output exceeds cutoff limit./

}