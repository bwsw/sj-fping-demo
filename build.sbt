name := "pingstation"

addCommandAlias("rebuild", ";clean; compile; package")

val commonSettings = Seq(
  version := "1.0",
  scalaVersion := Dependencies.Versions.scala,
  scalacOptions ++= Seq(
    "-unchecked",
    "-deprecation",
    "-feature"
  ),
  resolvers +=
    "Sonatype OSS" at "https://oss.sonatype.org/service/local/staging/deploy/maven2",

  libraryDependencies ++= Seq(
    "com.bwsw" % "sj-engine-core_2.11" % "1.0.0"),

  assemblyMergeStrategy in assembly := {
    case PathList("scala", xs@_*) => MergeStrategy.first
    case PathList("org", "slf4j", xs@_*) => MergeStrategy.first
    case PathList("io", "netty", xs@_*) => MergeStrategy.first
    case PathList("org", "joda", xs@_*) => MergeStrategy.first
    case "library.properties" => MergeStrategy.concat
    case "log4j.properties" => MergeStrategy.concat
    case x =>
      val oldStrategy = (assemblyMergeStrategy in assembly).value
      oldStrategy(x)
  },

  assemblyJarName in assembly := s"${name.value}-${version.value}.jar",

  fork in run := true,
  fork in Test := true,
  parallelExecution in Test := false
)

lazy val root = (project in file(".")) aggregate(psInput, psProcess, psOutput)

lazy val psInput = Project(id = "ps-input",
  base = file("ps-input"))
  .settings(commonSettings: _*)

lazy val psProcess = Project(id = "ps-process",
  base = file("ps-process"))
  .settings(commonSettings: _*)

lazy val psOutput = Project(id = "ps-output",
  base = file("ps-output"))
  .settings(commonSettings: _*)
