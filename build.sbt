name := "spark-streaming_example"

version := "1.0.0"

scalaVersion := "2.10.5"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.4.1",
  "org.apache.spark" % "spark-streaming_2.10" % "1.4.1",
  "org.apache.spark" % "spark-streaming-twitter_2.10" % "1.4.1",
  "org.apache.spark" %% "spark-streaming-kafka" % "1.4.1",
  "com.datastax.spark" %% "spark-cassandra-connector" % "1.4.0-M1"
)
