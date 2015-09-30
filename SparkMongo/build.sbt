name := "Simple Project"
version := "1.0"
scalaVersion := "2.10.3"
libraryDependencies ++= Seq("org.mongodb" % "mongo-hadoop-core" % "1.3.0" exclude("javax.servlet", "*"), "org.apache.spark" % "spark-core_2.10" % "1.2.0", "org.apache.spark" % "spark-streaming_2.10" % "1.2.0")
libraryDependencies += "org.mongodb" %% "casbah" % "2.8.2"