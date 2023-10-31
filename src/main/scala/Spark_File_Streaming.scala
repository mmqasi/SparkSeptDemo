package org.itc.com

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.functions._
import org.itc.com.SSFilestreaming.out

object Spark_File_Streaming extends App {

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "Project File Streaming")
  sparkConf.set("spark.master", "local[1]")
  sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true")
  sparkConf.set("spark.sql.streaming.schemaInference", "true")
  val spark = SparkSession.builder().config(sparkConf).getOrCreate()
  Logger.getLogger("org").setLevel(Level.ERROR)

  spark.sparkContext.setLogLevel("ERROR")

  val top_company = spark.readStream.format("csv").option("path", "Input").load()

  top_company.createOrReplaceTempView("salary_offered")

  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.streaming.OutputMode

  val query = spark.sql("SELECT _c0 AS Company_Name, COUNT(_c0) AS JobCount " +
    "FROM salary_offered " +
    "GROUP BY _c0, window(current_timestamp(), '1 minutes') " + // Adjust the window size as needed
    "ORDER BY JobCount DESC " +
    "LIMIT 1")

  query.writeStream.outputMode(OutputMode.Complete())
    .format("console")
    .start()
    .awaitTermination()

}