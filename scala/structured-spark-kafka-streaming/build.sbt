name := "structured-spark-kafka-streaming"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.6"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.6"

// in order to run without spark-submit in local
// should remove and use --jar or --package in product
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.6"

// seem Test doesn't work in this case, maybe used in UT
//libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.6" % Test