name := "Discount"

version := "3.0.1"

scalaVersion := "2.12.16"

val sparkVersion = "3.1.0"

//If compiling on JDK 8, the --release 8 flag can be safely removed (needed for backwards compatibility on later JDKs).
//Also applies to javacOptions below.
scalacOptions ++= Seq("--feature", "-release", "8")

javacOptions ++= Seq("--release=8")

//ThisBuild / scapegoatVersion := "1.4.17"

resolvers += "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven"

libraryDependencies += "org.rogach" %% "scallop" % "latest.integration"

libraryDependencies += "it.unimi.dsi" % "fastutil" % "latest.integration"

libraryDependencies += "org.scalatest" %% "scalatest" % "latest.integration" % "test"

libraryDependencies += "org.scalatestplus" %% "scalacheck-1-15" % "latest.integration" % "test"

//For Windows, to remove the dependency on winutils.exe for local filesystem access
libraryDependencies += "com.globalmentor" % "hadoop-bare-naked-local-fs" % "latest.integration"

//The "provided" configuration prevents sbt-assembly from including spark in the packaged jar.
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"

Compile / unmanagedResourceDirectories += { baseDirectory.value / "resources" }

//Do not run tests during the assembly task
//(Running tests manually is still recommended)
assembly / test := {}

//Do not include scala library JARs in assembly (provided by Spark)
assembly / assemblyOption := (assembly / assemblyOption).value.copy(includeScala = false)

//Run tests in a separate JVM
Test / fork := true

Test / javaOptions += "-Xmx4G"

Test / testOptions += Tests.Argument(TestFrameworks.ScalaCheck, "-verbosity", "1")
