name := "Spark learning"

version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies ++= 
List("org.apache.spark" %% "spark-core" % "1.6.1", 
"org.apache.spark" % "spark-sql_2.10" % "1.6.1",
"com.databricks" %% "spark-csv" % "1.4.0")
