package org.itc.com
import org.apache.log4j.{Level, Logger}
import org.apache.parquet.format.IntType
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.itc.com.DFDemo.{finaldf, orderSchema, spark, sparkConf}
import org.joda.time.DateTime
object Mini_Project_Spark extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "Project_mini_spark")
  sparkConf.set("spark.master", "local[1]")

  val spark = SparkSession.builder().config(sparkConf).getOrCreate()
  val table1DF = spark.read.option("header", "true").csv("I:\\Mini_Project\\sales.csv")
  val table2DF = spark.read.option("header", "true").csv("I:\\Mini_Project\\menu.csv")
  val table3DF = spark.read.option("header", "true").csv("I:\\Mini_Project\\members.csv")

  table1DF.createOrReplaceTempView("sales")
  table2DF.createOrReplaceTempView("menu")
  table3DF.createOrReplaceTempView("members")


  /*val joinedDF = table1DF.join(table2DF, "product_id").join(table3DF, "customer_id")
  joinedDF.write.mode("overwrite").option("header", "true").csv("I:\\Mini_Project\\final_output.csv")
  val orderSchema = StructType(List(StructField("CustomerID",StringType,true),StructField("ProductID",StringType,true),StructField("orderDate",StringType,true),StructField("ProductName",StringType,true),StructField("price",IntegerType,true),StructField("JoinDate",StringType,true)))

  */
  //val FinalDF = spark.read.option("header", true).schema(orderSchema).csv(path = "I:\\Mini_Project\\final_output.csv")
  ///FinalDF.show(3)
  //val nullCounts = FinalDF.columns.map(colName => colName -> FinalDF.filter(col(colName).isNull).count())
  //nullCounts.foreach { case (colName, count) =>
  //  println(s"Column $colName has $count null values")
  //}

  //*****//
  //Odd with Data Frame And Even with SQL queries
  //*****//

  //1. What is the total amount each customer spent at the restaurant?
  val joinedDF = table1DF.join(table2DF, table1DF("product_id") === table2DF("product_id"))
  val totalAmountSpentDF = joinedDF
    .groupBy("customer_id")
    .agg(sum("price").alias("total_amount_spent"))
    .orderBy("customer_id")
  totalAmountSpentDF.show()


  //02. How many days has each customer visited the restaurant ?

  spark.sql("""SELECT customer_id , COUNT(DISTINCT order_date) AS days_visited
  FROM sales
  GROUP BY customer_id
  ORDER BY customer_id""").show(5)

  //3. What was the first item from the menu purchased by each customer ?

  // Create a window specification for partitioning by customer_id and ordering by order_date
  val windowSpec = Window.partitionBy("customer_id").orderBy("order_date")

  // Add a row number to each row within the window partition
  val rankedSalesDF = table1DF.withColumn("row_num", row_number().over(windowSpec))

  // Filter for rows with row number = 1 to get the first purchase of each customer
  val firstPurchaseDF = rankedSalesDF.filter(col("row_num") === 1)

  // Join with menu data to get the item name
  val customerFirstItemDF = firstPurchaseDF
    .join(table2DF, firstPurchaseDF("product_id") === table2DF("product_id"))
    .select("customer_id", "product_name")

  customerFirstItemDF.show()

  //4. What is the most purchased item on the menu and how many times was it purchased by all customers ?

  val result4DF = spark.sql(
    """
    SELECT m.product_name, COUNT(*) AS purchase_count
    FROM sales s
    JOIN menu m
    ON s.product_id = m.product_id
    GROUP BY m.product_name
    ORDER BY purchase_count DESC
    LIMIT 1
    """
  )
  result4DF.show()

  //5. Which item was the most popular for each customer ?

  // Create a window specification for partitioning by customer_id and ordering by product_count
  val window2Spec = Window.partitionBy("customer_id").orderBy(col("product_count").desc)

  // Calculate the count of each product purchased by each customer
  val productCountDF = table1DF
    .groupBy("customer_id", "product_id")
    .count()
    .withColumnRenamed("count", "product_count")

  // Add a rank to each row within the window partition
  val rankedProductCountDF = productCountDF.withColumn("rank", rank().over(window2Spec))

  // Filter for rows with rank = 1 to get the most popular item for each customer
  val mostPopularItemDF = rankedProductCountDF.filter(col("rank") === 1)

  // Join with menu data to get the item name
  val customerMostPopularItemDF = mostPopularItemDF
    .join(table2DF, mostPopularItemDF("product_id") === table2DF("product_id"))
    .select("customer_id", "product_name")

  customerMostPopularItemDF.show()

  //6. Which item was purchased first by the customer after they became a member ?

  // Use Spark SQL to find the item purchased first after becoming a member
  val query6 =
    """
    SELECT s.customer_id, s.product_id, MIN(s.order_date) AS first_purchase_date
    FROM sales s
    JOIN members m ON s.customer_id = m.customer_id
    WHERE s.order_date > m.join_date
    GROUP BY s.customer_id, s.product_id
    """

  val resultQuery6 = spark.sql(query6)

  // Show the result
  resultQuery6.show()

  //7. Which item was purchased just before the customer became a member ?

  // Define a window specification to order the sales data by order date for each customer
  val windowSpecQuery7 = Window.partitionBy("customer_id").orderBy("order_date")

  // Create a DataFrame with the customer's first order date
  val firstOrderDateDF = table1DF.groupBy("customer_id").agg(min("order_date").alias("first_order_date"))

  // Join 'firstOrderDateDF' with 'sales' to get the purchase just before becoming a member
  val purchaseBeforeMembership = table1DF
    .join(firstOrderDateDF, Seq("customer_id"))
    .filter(col("order_date") < col("first_order_date"))
    .withColumn("days_before_membership", datediff(col("first_order_date"), col("order_date")))
    .withColumn("rank", rank().over(windowSpecQuery7))
    .filter(col("rank") === 1) // Select the purchase just before becoming a member

  // Show the result
  purchaseBeforeMembership.show()





  //8. What is the total items and amount spent for each member before they became a member ?


  //9. If each $1 spent equates to points and sushi has a 2 x points multiplier -how many points would each customer have ?

  val query9 =
    """
    SELECT
      s.customer_id,
      SUM(CASE WHEN m.product_name = 'sushi' THEN 2 * m.price ELSE m.price END) AS total_points
    FROM sales s
    JOIN menu m ON s.product_id = m.product_id
    GROUP BY s.customer_id
  """

  val customerPoints = spark.sql(query9)
  customerPoints.show()

  /*10. In the first week after a customer joins the program(including their join date) they earn 2 x points on all items ,
  not just sushi - how many points
  do customer A and B have at the end of January ?*/


  spark.stop()
}