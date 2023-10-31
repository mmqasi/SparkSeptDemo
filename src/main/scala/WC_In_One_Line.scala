package org.itc.com

import org.apache.spark.SparkContext

object WC_In_One_Line {
  def main(args: Array[String]): Unit = {

    val sc = new SparkContext()

    val fileRDD = sc.textFile(args(0)).flatMap(x => x.toLowerCase().split(" ")).map(x => (x, 1)).reduceByKey((x, y) => x + y)

    fileRDD.collect().foreach(println)


  }

}
