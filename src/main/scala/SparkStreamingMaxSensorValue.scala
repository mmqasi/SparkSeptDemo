package org.itc.com

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}

object SparkStreamingMaxSensorValue extends App {

    Logger.getLogger("org").setLevel(Level.ERROR)
    // Initialize SparkConf and StreamingContext
    val conf = new SparkConf().setAppName("SparkStreamingMaxSensorValue")setMaster("local[*]")


    val ssc = new StreamingContext(conf, Seconds(1))

    // Create a DStream from a source (e.g., a socket, Kafka, or a file)
    val lines: DStream[String] = ssc.socketTextStream("localhost", 9999) // Change the source as needed

    // Define a case class to represent sensor data
    private case class SensorData(dateTime: String, sensorName: String, tempValue: Double)

    // Parse the input data and convert it into a DataFrame
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    lines.foreachRDD { rdd =>
      val sensorDataDF = rdd
        .map(_.split(","))
        .map(attributes => SensorData(attributes(0), attributes(1), attributes(2).toDouble))
        .toDF()

      // Calculate the maximum temperature for each sensor
      val maxTemperatures = sensorDataDF
        .groupBy("sensorName")
        .agg(max("tempValue").as("maxTemperature"))

      // Print the maximum temperature for each sensor
      maxTemperatures.show()
    }

    // Start the Spark Streaming context
    ssc.start()
    ssc.awaitTermination()

}
