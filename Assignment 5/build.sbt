ThisBuild / version := "0.1"

ThisBuild / scalaVersion := "2.11.8"

scalaVersion := "2.11.8"
val sparkVersion = "2.3.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-graphx" % sparkVersion
)
lazy val root = (project in file("."))
  .settings(
    name := "Assignment5"
  )