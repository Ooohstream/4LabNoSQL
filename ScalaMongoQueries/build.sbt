name := "ScalaMongoQueries"

version := "0.1"

scalaVersion := "2.13.7"

libraryDependencies += "org.mongodb.scala" %% "mongo-scala-driver" % "4.3.4"

libraryDependencies ++= Seq("org.slf4j" % "slf4j-api" % "1.7.32",
  "org.slf4j" % "slf4j-simple" % "1.7.32")