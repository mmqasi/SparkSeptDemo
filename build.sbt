ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.11.10"

// https://mvnrepository.com/artifact/commons-io/commons-io
libraryDependencies += "commons-io" % "commons-io" % "2.6"
// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.3"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.3"
libraryDependencies += "org.apache.commons" % "commons-csv" % "1.8"

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.4.3"

libraryDependencies += "org.jfree" % "jfreechart" % "1.5.3"

libraryDependencies += "org.apache.kafka" % "kafka_2.11" % "0.11.0.3"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.4.8"

libraryDependencies += "org.apache.kafka" % "kafka_2.11" % "2.8.1"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.8",
  "org.apache.spark" %% "spark-streaming" % "2.4.8",
  "org.apache.spark" %% "spark-streaming-kafka-0-8" % "2.4.8", // Use the version compatible with your Spark version
  "org.apache.kafka" % "kafka-clients" % "2.8.0" // Use the latest Kafka version
)




lazy val root = (project in file("."))
  .settings(
    name := "SparkSeptDemo",
    idePackagePrefix := Some("org.itc.com")
  )
