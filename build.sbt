name := "airlines"

version := "0.1"

scalaVersion := "2.12.8"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.1"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.12" % "2.4.1"
