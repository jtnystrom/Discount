name := "Discount"

version := "2.2.1"

scalaVersion := "2.13.7"

scalacOptions += "-deprecation"

//ThisBuild / scapegoatVersion := "1.4.9"

resolvers += "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven"

libraryDependencies += "org.rogach" %% "scallop" % "latest.integration"

libraryDependencies += "org.scalatest" %% "scalatest" % "latest.integration" % "test"

libraryDependencies += "org.scalatestplus" %% "scalacheck-1-15" % "latest.integration" % "test"

//The "provided" configuration prevents sbt-assembly from including spark in the packaged jar.
//Change the version to compile for a different Spark version, e.g. 2.4.6
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.0" % "provided"

//Do not run tests during the assembly task
//(Running tests manually is still recommended)
assembly / test := {}

//Do not include scala library JARs in assembly (provided by Spark)
assembly / assemblyOption := (assembly / assemblyOption).value.copy(includeScala = false)

Test / testOptions += Tests.Argument(TestFrameworks.ScalaCheck, "-verbosity", "1")
