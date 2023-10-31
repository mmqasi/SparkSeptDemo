package org.itc.com

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.itc.com.DFDemo.{orderSchema, spark}
import org.itc.com.Mini_Project_Spark.sparkConf

//import scala.reflect.internal.util.NoSourceFile.path

object DSDemo extends App {

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name","DSDemo")
  sparkConf.set("spark.master","local[1]")
  val spark = SparkSession.builder().config(sparkConf).getOrCreate()

  case class orderdata(orderid:Int, orderdate:String, custid:Int,status:String)
  val ordersDDL = "orderid Int, orderdate String, custid Int, status String"
  val orderdf = spark.read.option("header", true).schema(ordersDDL).csv("C:\\Users\\mmqas\\Downloads\\orders.csv")
  import spark.implicits._
  val orderDS = orderdf.as[orderdata]

  //orderDS.show(2)
  orderDS.filter("orderid>1").show(2)


}
