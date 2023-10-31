package org.itc.com

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object StructuredStreamingWC extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "DFSeptDemo")
  sparkConf.set("spark.master", "local[1]")
  sparkConf.set("spark.streaming.stopGracefullyOnShutdown","true")

  val spark = SparkSession.builder().config(sparkConf).getOrCreate()

  val linesdf = spark.readStream.format("socket").option("host", "localhost").option("port", 9997).load()

  import spark.implicits._

  val wordsdf = linesdf.as[String].flatMap(_.split(" "))
  val wordCnt = wordsdf.groupBy("value").count()
  val producer1 = wordCnt.writeStream.outputMode("complete").format("console").option("checkpoint","checkpointloc1").trigger(Trigger.ProcessingTime(1)).start()
  producer1.awaitTermination()

}
