package org.itc.com
import org.apache.log4j.{Level, Logger}
import org.apache.parquet.format.IntType
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.kafka._


// Import the necessary Python libraries for PySpark and Pandas
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.itc.com.DFDemo.{finaldf, orderSchema, spark, sparkConf}
import org.joda.time.DateTime

import org.jfree.chart.{ChartFactory, ChartUtils, JFreeChart}
import org.jfree.chart.plot.PlotOrientation
import org.jfree.data.category.DefaultCategoryDataset
object Project extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "Project")
  sparkConf.set("spark.master", "local[1]")

  val spark = SparkSession.builder().config(sparkConf).getOrCreate()
  val DatasetDF = spark.read.option("header", "true").csv("C:\\Users\\mmqas\\OneDrive\\Desktop\\Project2023\\Job_Analysis.csv")
  DatasetDF.show(5)

  //Data Pre-processing

  val updatedDFSalary = DatasetDF.withColumn("Salary_Offered", col("Salary_Offered") + 15999)
  updatedDFSalary.show(5)

  // Create a list of column expressions to count null values in each column
  val columnExpressions = updatedDFSalary.columns.map(c => sum(when(col(c).isNull, 1).otherwise(0)).alias(s"${c}_null_count"))

  // Use select to calculate null counts for each column
  val nullCountsDF = updatedDFSalary.agg(columnExpressions.head, columnExpressions.tail: _*)

  // Show the result
  nullCountsDF.show()

  val PreprocessedDDF = updatedDFSalary.drop("State")
  PreprocessedDDF.show(3)

  // Create a list of column expressions to count null values in each column
  val columnExpressions_updated = PreprocessedDDF.columns.map(c => sum(when(col(c).isNull, 1).otherwise(0)).alias(s"${c}_null_count"))

  // Use select to calculate null counts for each column
  val nullCountsDF_updated = PreprocessedDDF.agg(columnExpressions_updated.head, columnExpressions_updated.tail: _*)

  // Show the result
  nullCountsDF_updated.show()

  // Queries
  // Queries
  // Queries
  // Show the company with the most job postings
  // Group the DataFrame by the "Company_Name" column and count the number of postings
  val companyJobCounts = PreprocessedDDF.groupBy("Company_Name")
    .agg(count("*").alias("JobCount"))

  // Find the company with the most job postings
  val companyWithMostJobs = companyJobCounts.orderBy(desc("JobCount")).limit(1)

  // Show the company with the most job postings
  companyWithMostJobs.show()


  // Create a dataset with company names and their job counts
  val dataset1 = new DefaultCategoryDataset()
  companyWithMostJobs.collect().foreach { row =>
    val company = row.getString(0)
    val jobCount = row.getLong(1)
    dataset1.addValue(jobCount, "Job Count", company)
  }

  // Create a bar chart
  val chart1: JFreeChart = ChartFactory.createBarChart(
    "Companies with the Most Job Postings",
    "Company",
    "Job Count",
    dataset1,
    PlotOrientation.VERTICAL,
    true, true, false

  )

  // Save the chart as an image file
  ChartUtils.saveChartAsPNG(new java.io.File("job_postings_bar_chart.png"), chart1, 800, 600)

  // Queries
  // Queries
  // Queries
  // Find the top 3 companies with the most job postings
  val top3Companies = companyJobCounts.orderBy(desc("JobCount")).limit(3)

  // Create a dataset with the top 3 companies and their job counts
  val dataset2 = new DefaultCategoryDataset()
  top3Companies.collect().foreach { row =>
    val company = row.getString(0)
    val jobCount = row.getLong(1)
    dataset2.addValue(jobCount, "Job Count", company)
  }

  // Create a bar chart
  val chart2: JFreeChart = ChartFactory.createBarChart(
    "Top 3 Companies with the Most Job Postings",
    "Company",
    "Job Count",
    dataset2,
    PlotOrientation.VERTICAL,
    true, true, false
  )

  // Save the chart as an image file
  ChartUtils.saveChartAsPNG(new java.io.File("top3_companies_job_postings.png"), chart2, 800, 600)

  // Queries
  // Queries
  // Queries

  // Find the top 3 highest-paid jobs
  val top3HighestPaidJobs = PreprocessedDDF.orderBy(desc("Salary_Offered")).limit(3)
  top3HighestPaidJobs.show()

  // Create a dataset with job titles and their corresponding salaries
  val dataset3 = new DefaultCategoryDataset()
  top3HighestPaidJobs.collect().foreach { row =>
    val jobTitle = row.getString(1)
    val salary = row.getDouble(5) // Assuming the salary column is of Double type
    val salaryRounded = Math.round(salary) // Round to the nearest integer
    dataset3.addValue(salaryRounded, "Salary", jobTitle)
  }

  // Create a bar chart
  val chart3: JFreeChart = ChartFactory.createBarChart(
    "Top 3 Highest-Paid Jobs",
    "Job Title",
    "Salary",
    dataset3,
    PlotOrientation.VERTICAL,
    true, true, false
  )

  // Save the chart as an image file
  ChartUtils.saveChartAsPNG(new java.io.File("top3_highest_paid_jobs.png"), chart3, 800, 600)

  // Queries
  // Queries
  // Queries
  // Top demanding skills

  // Split the "Skills" column into individual skills and count their occurrences
  val skillDemandDF = PreprocessedDDF
    .withColumn("Skills", split(col("Skills"), "\\s*,\\s*"))
    .withColumn("Skill", explode(col("Skills")))
    .groupBy("Job_Title", "Skill")
    .count()

  val windowSpec = Window.partitionBy("Job_Title").orderBy(desc("count"))

  val topDemandingSkillsDF = skillDemandDF
    .withColumn("rank", rank().over(windowSpec))
    .filter(col("rank") <= 3)
    .drop("rank")

  //topDemandingSkillsDF.show()

  val dataset4 = new DefaultCategoryDataset()

  topDemandingSkillsDF.collect().foreach { row =>
    val jobTitle = row.getString(0)
    val skill = row.getString(1)
    val count = row.getLong(2)

    dataset4.addValue(count, jobTitle, skill)
  }

  // Create a bar chart
  val chart4: JFreeChart = ChartFactory.createBarChart(
    "Top Demanding Skills for Each Job Title",
    "Skills",
    "Demand Count",
    dataset4,
    PlotOrientation.VERTICAL,
    true, true, false
  )

  // Save the chart as an image file
  ChartUtils.saveChartAsPNG(new java.io.File("top_demanding_skills.png"), chart4, 800, 600)


  // Queries
  // Queries
  // Queries
  //Top posted job
  // Group the DataFrame by job title and count the number of job postings
  val jobPostCountsDF = PreprocessedDDF.groupBy("Job_Title").count()
  // Find the job with the highest count
  val topPostedJob = jobPostCountsDF.orderBy(desc("count")).limit(1)
  topPostedJob.show()

  // Create a dataset for the bar chart
  val dataset5 = new DefaultCategoryDataset()

  topPostedJob.collect().foreach { row =>
    val jobTitle = row.getString(0) // Assuming job title is at index 0
    val postCount = row.getLong(1) // Assuming count is at index 1

    // Add the job to the dataset
    dataset5.addValue(postCount, "Job Post Count", jobTitle)
  }

  // Create a bar chart
  val chart5: JFreeChart = ChartFactory.createBarChart(
    "Top Posted Job",
    "Job Title",
    "Post Count",
    dataset5,
    PlotOrientation.VERTICAL,
    true, true, false
  )

  // Save the chart as an image file
  ChartUtils.saveChartAsPNG(new java.io.File("top_posted_job.png"), chart5, 800, 600)

  //Queries
  //Queries
  //Queries
  //Jobs posted in last 5 days

  // Calculate the date 5 days ago from the current date
  val fiveDaysAgo = date_sub(current_date(), 5)

  val jobsPostedInLast5Days = PreprocessedDDF.filter(col("Post_Date") >= fiveDaysAgo)
  jobsPostedInLast5Days.show(5)

  // Create a dataset for the bar chart
  val dataset6 = new DefaultCategoryDataset()

  jobsPostedInLast5Days.collect().foreach { row =>
    val jobTitle = row.getString(1) // Assuming job title is at index 1
    val postDate = row.getString(4) // Assuming post date is at index 4

    // Add the job to the dataset with the post date as the category
    dataset6.addValue(1, jobTitle, postDate)
  }

  // Create a bar chart
  val chart6: JFreeChart = ChartFactory.createBarChart(
    "Jobs Posted in the Last 5 Days",
    "Job Title",
    "Count",
    dataset6,
    PlotOrientation.VERTICAL,
    true, true, false
  )

  // Save the chart as an image file
  ChartUtils.saveChartAsPNG(new java.io.File("jobs_posted_last_5_days.png"), chart6, 800, 600)

  //Job Posted 1 day ago

  // Calculate the one day ago from the current date
  val oneDayAgo = date_sub(current_date(), 5)

  val jobsPostedYesterday = PreprocessedDDF.filter(col("Post_Date") >= oneDayAgo)
  jobsPostedYesterday.show(5)

  // Create a dataset for the bar chart
  val dataset7 = new DefaultCategoryDataset()

  jobsPostedYesterday.collect().foreach { row =>
    val jobTitle = row.getString(1) // Assuming job title is at index 1
    val postDate = row.getString(4) // Assuming post date is at index 4

    // Add the job to the dataset with the post date as the category
    dataset7.addValue(1, jobTitle, postDate)
  }

  // Create a bar chart
  val chart7: JFreeChart = ChartFactory.createBarChart(
    "Jobs Posted in the Last 5 Days",
    "Job Title",
    "Count",
    dataset7,
    PlotOrientation.VERTICAL,
    true, true, false
  )

  // Save the chart as an image file
  ChartUtils.saveChartAsPNG(new java.io.File("jobs_posted_Yesterday.png"), chart7, 800, 600)


  //Query
  //Query
  //Query
  //Top 5 posted jobs yesterday

  // Calculate the date for yesterday and today
  val yesterday = date_sub(current_date(), 1)
  val today = current_date()

  val top5jobsPostedYesterday = PreprocessedDDF.filter(col("Post_Date") === yesterday)

  // Group the DataFrame by job title and count the number of job postings
  val top5jobPostCountsDF = top5jobsPostedYesterday.groupBy("Job_Title").count()

  // Rank the job postings by count in descending order
  val rankedJobPosts = jobPostCountsDF.withColumn("rank", dense_rank().over(Window.orderBy(desc("count"))))

  // Select the top 5 job postings from yesterday
  val top5PostedJobsYesterday = rankedJobPosts.filter(col("rank") <= 5)

  // Create a dataset for the bar chart
  val dataset8 = new DefaultCategoryDataset()

  top5PostedJobsYesterday.collect().foreach { row =>
    val jobTitle = row.getString(0) // Assuming job title is at index 0
    val postCount = row.getLong(1) // Assuming count is at index 1

    // Add the job to the dataset
    dataset8.addValue(postCount, "Job Post Count", jobTitle)
  }

  // Create a bar chart
  val chart8: JFreeChart = ChartFactory.createBarChart(
    "Top 5 Posted Jobs Yesterday",
    "Job Title",
    "Post Count",
    dataset8,
    PlotOrientation.VERTICAL,
    true, true, false
  )

  // Save the chart as an image file
  ChartUtils.saveChartAsPNG(new java.io.File("top5_posted_jobs_yesterday.png"), chart8, 800, 600)


}

