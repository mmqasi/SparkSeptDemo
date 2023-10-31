package org.itc.com

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object joinDemo extends App{
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "DSDemo")
  sparkConf.set("spark.master", "local[1]")
  val spark = SparkSession.builder().config(sparkConf).getOrCreate()

  import spark.implicits._

  val empDF = Seq(("Rupali", "Mumbai"), ("Mohsin", "pune"), ("Devi", "bangalore"), ("Alex", "hyderabad")).toDF("fName", "city")

  val citiesDF = Seq(("Mumbai", "India"), ("pune", "India"), ("bangalore", "India"), ("hyderabad", "India"))
    .toDF("city", "country")

  val joindf = empDF.join(citiesDF,empDF.col("city")===citiesDF.col("city"),"inner")
  val update_joindf = joindf.drop(empDF.col("city"))
  update_joindf.select("city","fName","country").show(3)

}
