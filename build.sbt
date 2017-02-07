name := "CDRec"

version := "1.0"

scalaVersion := "2.10.4"

//for Spark
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.0",
  "org.apache.spark" %% "spark-mllib" % "1.6.0",
  "com.github.fommil.netlib" % "all" % "1.1.2" pomOnly()
)

//for ScalaTest
libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.1"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"