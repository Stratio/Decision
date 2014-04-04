import AssemblyKeys._

name := "stratio-streaming-api"

organization := "com.stratio"

version := "0.0.1-SNAPSHOT"

scalaVersion := "2.10.3"

seq(assemblySettings: _*)

jarName in assembly := "stratio-streaming-api-1.0.0-SNAPSHOT.jar"

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
    case PathList(ps @ _*) if ps.last endsWith ".properties" => MergeStrategy.first
    case PathList(ps @ _*) if ps.last endsWith ".xml" => MergeStrategy.first
    case PathList("scala", xs @ _*)         => MergeStrategy.first
    case PathList("org","apache", xs @ _*)         => MergeStrategy.first
    case PathList("org","slf4j", xs @ _*)         => MergeStrategy.first
    case PathList("org","xerial", xs @ _*)         => MergeStrategy.first
    case x => old(x)
  }
}

test in assembly := {}

resolvers += "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"

libraryDependencies ++= Seq(
  "org.scalatest" % "scalatest_2.10" % "2.0" % "test" withSources() withJavadoc(),
  "org.apache.kafka" % "kafka_2.10" % "0.8.1" exclude("com.sun.jdmk", "jmxtools") exclude("com.sun.jmx", "jmxri"),
  "com.typesafe" % "config" % "1.2.0",
  "org.apache.curator" % "curator-framework" % "2.4.1",
  "org.slf4j" % "slf4j-log4j12" % "1.7.5",
  "com.stratio.streaming" % "StratioStreamingCommons" % "1.0-SNAPSHOT",
  "com.google.code.gson" % "gson" % "2.2.4"
)

initialCommands := "import com.stratio.bus._"

