import AssemblyKeys._
import SonatypeKeys._
import aether.Aether._

sonatypeSettings

useGpg := true

pgpReadOnly := false

moduleName := "streaming-api"

name := "streaming-api"

crossPaths := false

organization := "com.stratio.streaming"

profileName := "com.stratio"

publishMavenStyle := true

version := "0.2.0-SNAPSHOT"

scalaVersion := "2.10.3"

seq(assemblySettings: _*)

jarName in assembly := "stratio-streaming-api-0.2.0-SNAPSHOT.jar"

addArtifact(Artifact("streaming-api"), sbtassembly.Plugin.AssemblyKeys.assembly)

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
    case PathList(ps @ _*) if ps.last endsWith ".properties" => MergeStrategy.first
    case PathList(ps @ _*) if ps.last endsWith ".xml" => MergeStrategy.first
    case PathList("scala", xs @ _*)         => MergeStrategy.first
    case PathList("org","apache", xs @ _*)         => MergeStrategy.first
    case PathList("org","slf4j", xs @ _*)         => MergeStrategy.first
    case PathList("org","xerial", xs @ _*)         => MergeStrategy.first
    case PathList("org","jboss","netty", xs @ _*)         => MergeStrategy.first
    case x => old(x)
  }
}

test in assembly := {}

resolvers ++= Seq(
   "Nexus snapshots" at "http://nexus.strat.io:8081/nexus/content/repositories/snapshots",
   "Nexus releases" at "http://nexus.strat.io:8081/nexus/content/repositories/releases",
   "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"
)

libraryDependencies ++= Seq(
  "org.scalatest" % "scalatest_2.10" % "2.0" % "test",
  "com.datastax.cassandra" % "cassandra-driver-core" % "2.0.2" exclude("org.jboss", "netty"),
  "org.apache.kafka" % "kafka_2.10" % "0.8.1" exclude("com.sun.jdmk", "jmxtools") exclude("com.sun.jmx", "jmxri") exclude("org.apache.zookeeper", "zookeeper"),
  "com.typesafe" % "config" % "1.2.0",
  "org.apache.curator" % "curator-framework" % "2.4.1",
  "org.slf4j" % "slf4j-log4j12" % "1.7.5",
  "com.stratio.streaming" % "streaming-commons" % "0.3.0-SNAPSHOT",
  "com.google.code.gson" % "gson" % "2.2.4",
  "org.scalaj" %% "scalaj-http" % "0.3.15"
)

credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")

publishTo := {
  if (version.value.trim.endsWith("SNAPSHOT"))
    Some("snapshots" at "http://nexus.strat.io:8081/nexus/content/repositories/snapshots")
  else
    Some("releases"  at "https://oss.sonatype.org/service/local/staging/deploy/maven2")
}

aetherPublishBothSettings

// To sync with Maven central, you need to supply the following information:
pomExtra := {
  <url>http://www.stratio.com</url>
  <licenses>
    <license>
      <name>The Apache Software License, Version 2.0</name>
      <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
    </license>
  </licenses>
  <scm>
    <connection>scm:git:git@github.com/Stratio/stratio-streaming-api.git</connection>
    <developerConnection>scm:git:git@github.com:Stratio/stratio-streaming-api.git</developerConnection>
    <url>https://github.com/Stratio/stratio-streaming-api</url>
  </scm>
  <developers>
    <developer>
      <id>dmorales</id>
      <name>David Morales</name>
      <email>dmorales@stratio.com</email>
      <roles>
        <role>architect</role>
        <role>developer</role>
        <role>maintainer</role>
      </roles>
    </developer>
    <developer>
      <id>arodriguez</id>
      <name>Alberto Rodriguez</name>
      <email>arodriguez@stratio.com</email>
      <roles>
        <role>architect</role>
        <role>developer</role>
        <role>maintainer</role>
      </roles>
    </developer>
  </developers>
}

initialCommands := "import com.stratio.streaming.api._"

