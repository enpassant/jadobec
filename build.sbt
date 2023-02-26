name := """jadobec"""

version := "1.0.0-SNAPSHOT"

description := "JDBC wrapper for very simple, functional database handling."

lazy val publishSettings = Seq(
  publishMavenStyle := true,
  publishTo := {
    val nexus = "https://s01.oss.sonatype.org/"
    if (isSnapshot.value)
      Some("snapshots" at nexus + "content/repositories/snapshots")
    else
      Some("releases"  at nexus + "service/local/staging/deploy/maven2")
  },
  publishConfiguration := publishConfiguration.value.withOverwrite(true),
  Test / publishArtifact := false,
  pomIncludeRepository := { _ => false },
  pomExtra := (
    <url>https://github.com/enpassant/jazio</url>
    <licenses>
      <license>
        <name>Apache-style</name>
        <url>https://opensource.org/licenses/Apache-2.0</url>
        <distribution>repo</distribution>
      </license>
    </licenses>
    <scm>
      <url>git@github.com:enpassant/jadobec.git</url>
      <connection>scm:git:git@github.com:enpassant/jadobec.git</connection>
    </scm>
    <developers>
      <developer>
        <id>enpassant</id>
        <name>Enpassant</name>
        <email>enpassant.prog@gmail.com</email>
        <url>https://github.com/enpassant</url>
      </developer>
    </developers>)
)

lazy val root = (project in file("."))
  .configs(IntegrationTest)
  .settings(
    publishSettings,
    //javacOptions += "-Xlint:unchecked",
    javaOptions += "-Xmx512m",
    organization := "io.github.enpassant",
    libraryDependencies ++= Seq(
      "io.github.enpassant" % "jazio" % "1.0.1-SNAPSHOT",
      "org.postgresql" % "postgresql" % "42.2.10" % Test,
      "com.h2database" % "h2" % "2.1.214" % Test,
      "junit" % "junit" % "4.12" % Test,
      "com.novocode" % "junit-interface" % "0.11" % Test
    ),
    crossPaths := false,
    testOptions += Tests.Argument(TestFrameworks.JUnit),
    Test / parallelExecution := false
  )
