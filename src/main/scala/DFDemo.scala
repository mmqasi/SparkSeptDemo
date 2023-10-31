package org.itc.com

import org.apache.spark.SparkConf
import org.apache.spark
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{SaveMode, SparkSession, functions}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
object DFDemo extends App{
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name","DFSeptDemo")
  sparkConf.set("spark.master","local[1]")

  val spark = SparkSession.builder().config(sparkConf).getOrCreate()
  //val ordersDDL = "orderid Int, orderDate String, custid Int, status String"
  // val orderdf = spark.read.option("header",true).option("inferSchema",true).csv("C:\\Users\\mmqas\\Downloads\\orders.csv")
  //Developer can give schema while writting code
  //val orderdf = spark.read.option("header",true).schema(ordersDDL).csv("C:\\Users\\mmqas\\Downloads\\orders.csv")
  //orderdf.show(5)



  //Way 2

  //val orderSchema = StructType(List(StructField("Orderid",IntegerType,true),StructField("Orderdate",StringType,true),StructField("Customerid",IntegerType,true),StructField("status",StringType,true)))
  //val orderSchema = StructType(List(StructField("Orderid",IntegerType,true),StructField("Orderdate",StringType,true),StructField("Customerid",IntegerType,true),StructField("status",IntegerType,true)))
  //val orderdf = spark.read.option("header",true).schema(orderSchema).csv("C:\\Users\\mmqas\\Downloads\\orders.csv")
  //orderdf.show(5)

  val orderSchema = StructType(List(StructField("Orderid", IntegerType, true), StructField("Orderdate", StringType, true), StructField("Customerid", IntegerType, true), StructField("status", StringType, true)))
  val orderdf = spark.read.option("header", true).schema(orderSchema).csv(path = "C:\\Users\\mmqas\\Downloads\\orders.csv")
  //orderdf.show(5)

  //Processing
  //orderdf.where("Customerid<10000").show(3)
  //orderdf.where("Customerid<10000").groupBy("status").agg(functions.min("Orderid"),functions.max("Orderid")).show(5)
  //println("Minimum orderid is :  " + orderdf.agg(functions.min("Orderid")).head().get(0))
  //println("Maximum orderid is :  " + orderdf.agg(functions.max("Orderid")).head().get(0))

  //orderdf.withColumn("discount", lit(100)).show(10)
  //Update OrderId
  //orderdf.withColumn("Orderid",col("Orderid")+10).show(5)
  val finaldf = orderdf.withColumn("NewOrderid",col("Orderid")+10)
  //finaldf.write.format("csv").mode(SaveMode.Overwrite).option("path","C:\\Users\\mmqas\\Downloads\\final").save()
  //finaldf.write.csv("C:\\Users\\mmqas\\Downloads\\final")
  finaldf.repartition(2).write.format("csv").mode(SaveMode.Overwrite).option("path","C:\\Users\\mmqas\\Downloads\\final1").save()
  finaldf.coalesce(1).write.format("csv").mode(SaveMode.Overwrite).option("path","C:\\Users\\mmqas\\Downloads\\final2").save()
  finaldf.createOrReplaceTempView("table1")
  spark.sql("Select * from table1 where orderid < 10").show()


  spark.stop()
}
