name := "SparkWithCassandra"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "1.4.1"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "1.4.1"
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "1.4.0-M3"