import AssemblyKeys._

moduleName := "StratioStreamingAPI"

crossPaths := false

organization := "com.stratio.streaming"

publishMavenStyle := true

version := "0.0.1-SNAPSHOT"

scalaVersion := "2.10.3"

seq(assemblySettings: _*)

jarName in assembly := "StratioStreamingAPI-1.0.0-SNAPSHOT.jar"

addArtifact(Artifact("StratioStreamingAPI"), sbtassembly.Plugin.AssemblyKeys.assembly)

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

resolvers += "Nexus snapshots" at "http://nexus.strat.io:8081/nexus/content/repositories/snapshots"

libraryDependencies ++= Seq(
  "org.scalatest" % "scalatest_2.10" % "2.0" % "test",
  "org.apache.kafka" % "kafka_2.10" % "0.8.1" exclude("com.sun.jdmk", "jmxtools") exclude("com.sun.jmx", "jmxri"),
  "com.typesafe" % "config" % "1.2.0",
  "org.apache.curator" % "curator-framework" % "2.4.1",
  "org.slf4j" % "slf4j-log4j12" % "1.7.5",
  "com.stratio.streaming" % "StratioStreamingCommons" % "0.0.1-SNAPSHOT",
  "com.google.code.gson" % "gson" % "2.2.4"
)

credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")

publishTo := {
  val nexus = "http://nexus.strat.io:8081/nexus/content/repositories/"
  if (version.value.trim.endsWith("SNAPSHOT"))
    Some("snapshots" at nexus + "snapshots")
  else
    Some("releases"  at nexus + "releases")
}

initialCommands := "import com.stratio.bus._"

