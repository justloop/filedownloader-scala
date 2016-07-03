

name := "filedownloader-scala"

version := "1.0"

scalaVersion := "2.11.8"
lazy val akkaVersion = "2.4.7"

fork in Test := true

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-cluster" % akkaVersion,
  "com.typesafe.akka" %% "akka-cluster-tools" % akkaVersion,
  "com.typesafe.akka" %% "akka-persistence" % akkaVersion,
  "org.iq80.leveldb" % "leveldb" % "0.7",
  "org.fusesource.leveldbjni" % "leveldbjni-all" % "1.8",
  "commons-net" % "commons-net" % "3.5",
  "commons-vfs" % "commons-vfs" % "1.0",
  "commons-io" % "commons-io" % "2.5",
  "com.jcraft" % "jsch" % "0.1.53",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.4.0",
  "ch.qos.logback" %  "logback-classic" % "1.1.7",
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test",
  "commons-io" % "commons-io" % "2.4" % "test")
    