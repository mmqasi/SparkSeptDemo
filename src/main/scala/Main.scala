package org.itc.com

import org.apache.spark.SparkContext

object Main {
  def main(args: Array[String]): Unit = {
    println("Hello world!")
    val sc = new SparkContext("local[*]","MyFirstSPARK_Application")

    val fileRDD = sc.textFile(path = "C:\\Users\\mmqas\\OneDrive\\Desktop\\Data.txt")
    //accepts one line and return many words 1 to M
    //It will take line by line and will read whole file
    val wordsRDD = fileRDD.flatMap(x => x.split(" "))
    //1 to 1
    val wordRDD = wordsRDD.map(x => (x.toLowerCase(),1))
    //Aggregation
    val finaloutput = wordRDD.reduceByKey((x,y) => x+y).sortBy(_._2, ascending = false)
    //print final output to screen.
    finaloutput.collect().foreach(println)
  }
}
