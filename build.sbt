name := "akka-quickstart-scala"

version := "1.0"

scalaVersion := "2.12.2"

lazy val akkaVersion = "2.5.3"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "org.scalatest" %% "scalatest" % "3.0.1" % "test",
  "com.clevercloud" %% "warp10-scala-client" % "2.0.2"
)
resolvers += "Clever Cloud Bintray" at "https://dl.bintray.com/clevercloud/maven"