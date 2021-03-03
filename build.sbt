name := "Discount"

version := "1.3.0"

//Change to compile for a different scala version, e.g. 2.12.12
scalaVersion := "2.11.12"

resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"

libraryDependencies += "org.rogach" %% "scallop" % "latest.integration"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8"

//Change to compile for a different Spark version, e.g. 3.0.1
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.6"

