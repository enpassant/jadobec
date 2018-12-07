name := """jadobec"""

version := "0.1.0-SNAPSHOT"

description := "JDBC wrapper for very simple, functional database handling."

javaOptions += "-Xmx512m"

libraryDependencies ++= Seq(
  "com.h2database" % "h2" % "1.4.197",
  "junit" % "junit" % "4.12" % Test,
  "com.novocode" % "junit-interface" % "0.11" % Test
)

testOptions += Tests.Argument(TestFrameworks.JUnit)

