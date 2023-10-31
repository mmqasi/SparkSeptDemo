package org.itc.com

import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.nio.file.Files.lines

object SparkStreamingWC extends App {

  val sc = new SparkContext("local[*]","WCstreamingcontext")



  val ssc = new StreamingContext(sc,Seconds(2))

  val lines = ssc.socketTextStream("localhost",9998)
  val wordCount = lines.flatMap(x => x.toLowerCase().split(" ")).map(x => (x, 1)).reduceByKey((x, y) => x + y)
  wordCount.print()
  ssc.start()
  ssc.awaitTermination()  //Keep application running
}
