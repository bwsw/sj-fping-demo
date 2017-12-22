name := "sj-fping-demo"

val demoVersion = "1.0-SNAPSHOT"

addCommandAlias("rebuild", ";clean; compile; package")

val commonSettings = Seq(
  version := demoVersion,
  scalaVersion := Dependencies.Versions.scala,
  scalacOptions ++= Seq(
    "-unchecked",
    "-deprecation",
    "-feature"
  ),

  licenses := Seq("Apache 2" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
  homepage := Some(url("https://github.com/bwsw/sj-fping-demo.git")),
  pomIncludeRepository := { _ => false },
  parallelExecution in Test := false,
  organization := "com.bwsw",
  publishMavenStyle := true,
  pomIncludeRepository := { _ => false },
  isSnapshot := true,

  pomExtra :=
    <scm>
      <url>git@github.com/bwsw/sj-fping-demo.git</url>
      <connection>scm:git@github.com/bwsw/sj-fping-demo.git</connection>
    </scm>
      <developers>
        <developer>
          <id>bitworks</id>
          <name>Bitworks Software, Ltd.</name>
          <url>http://bitworks.software/</url>
        </developer>
      </developers>,


  publishTo in ThisBuild := {
    val nexus = "https://oss.sonatype.org/"
    if (isSnapshot.value)
      Some("snapshots" at nexus + "content/repositories/snapshots")
    else
      Some("releases" at nexus + "service/local/staging/deploy/maven2")
  },
  publishArtifact in Test := false,


  resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
  resolvers += "Elasticsearch Releases" at "https://artifacts.elastic.co/maven",

  libraryDependencies ++= Dependencies.sjFpingDependencies.value,

  assemblyMergeStrategy in assembly := {
    case PathList("org", "apache", "commons", "logging", xs@_*) => MergeStrategy.first
    case x if x.endsWith("io.netty.versions.properties") => MergeStrategy.concat
    case "log4j.properties" => MergeStrategy.concat
    case PathList("io", "netty", xs@_*) => MergeStrategy.first
    case x =>
      val oldStrategy = (assemblyMergeStrategy in assembly).value
      oldStrategy(x)
  },

  assemblyJarName in assembly := s"${name.value}-${version.value}.jar",

  fork in run := true,
  fork in Test := true,
  parallelExecution in Test := false
)

lazy val root = (project in file(".")).settings(publish := {}) aggregate(psProcess, psOutput)

lazy val psProcess = Project(
  id = "ps-process",
  base = file("ps-process"))
  .settings(commonSettings: _*)

lazy val psOutput = Project(
  id = "ps-output",
  base = file("ps-output"))
  .settings(commonSettings: _*)
