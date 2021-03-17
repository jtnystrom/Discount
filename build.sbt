name := "Discount"

version := "1.3.0"

//Change to compile for a different scala version, e.g. 2.12.12
scalaVersion := "2.11.12"

//scapegoatVersion in ThisBuild := "1.3.9"
resolvers += "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven"

libraryDependencies += "org.rogach" %% "scallop" % "latest.integration"

//The "provided" configuration keeps these dependencies from being included in the deployment jar
//(packaged by sbt-assembly)
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % "provided"

//Change to compile for a different Spark version, e.g. 3.0.1
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.6" % "provided"

//Do not run tests during the assembly task
//(Running tests manually is still recommended)
test in assembly := {}

//Do not include scala library JARs in assembly (provided by Spark)
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
