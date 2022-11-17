name := "Discount"

version := "2.3.0"

scalaVersion := "2.13.10"

scalacOptions ++= Seq("-deprecation", "-feature", "-release:8")

//ThisBuild / scapegoatVersion := "1.4.17"

resolvers += "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven"

val sparkVersion = "3.2.0"

libraryDependencies += "org.rogach" %% "scallop" % "latest.integration"

libraryDependencies += "it.unimi.dsi" % "fastutil" % "latest.integration"

libraryDependencies += "org.scalatest" %% "scalatest" % "latest.integration" % "test"

libraryDependencies += "org.scalatestplus" %% "scalacheck-1-15" % "latest.integration" % "test"

//For Windows, to remove the dependency on winutils.exe for local filesystem access
libraryDependencies += "com.globalmentor" % "hadoop-bare-naked-local-fs" % "latest.integration"

//The "provided" configuration prevents sbt-assembly from including spark in the packaged jar.
//Change the version to compile for a different Spark version, e.g. 2.4.6
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"

Compile / unmanagedResourceDirectories += { baseDirectory.value / "resources" }

//Do not run tests during the assembly task
//(Running tests manually is still recommended)
assembly / test := {}

//Do not include scala library JARs in assembly (provided by Spark)
assembly / assemblyOption := (assembly / assemblyOption).value.copy(includeScala = false)

Test / fork := true

Test / javaOptions ++= Seq("-Xmx4G")

Test / testOptions += Tests.Argument(TestFrameworks.ScalaCheck, "-verbosity", "1")
