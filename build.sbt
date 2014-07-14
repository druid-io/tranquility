organization := "com.metamx"

name := "tranquility"

scalaVersion := "2.9.1"

crossScalaVersions := Seq("2.9.1", "2.10.4")

lazy val root = project.in(file("."))

net.virtualvoid.sbt.graph.Plugin.graphSettings

resolvers ++= Seq(
  "Metamarkets Releases" at "https://metamx.artifactoryonline.com/metamx/libs-releases/"
)

publishMavenStyle := true

publishTo := Some("pub-libs" at "https://metamx.artifactoryonline.com/metamx/pub-libs-releases-local")

parallelExecution in Test := false

fork in Test := true

publishArtifact in (Test, packageBin) := true

testOptions += Tests.Argument(TestFrameworks.JUnit, "-v", "-Duser.timezone=UTC")

javaOptions += "-Duser.timezone=UTC"

// storm-core has a package and object with the same name
scalacOptions += "-Yresolve-term-conflict:object"

releaseSettings

// When updating Jackson, watch out for: https://github.com/FasterXML/jackson-module-scala/issues/148
val jacksonFasterxmlVersion = "2.2.2"
val druidVersion = "0.6.121"

libraryDependencies ++= Seq(
  "com.metamx" %% "scala-util" % "1.8.20" force()
)

libraryDependencies ++= Seq(
  "org.slf4j" % "slf4j-api" % "1.7.2" force() force(),
  "org.slf4j" % "jul-to-slf4j" % "1.7.2" force() force()
)

libraryDependencies ++= Seq(
  "com.fasterxml.jackson.core" % "jackson-core" % jacksonFasterxmlVersion force(),
  "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonFasterxmlVersion force(),
  "com.fasterxml.jackson.core" % "jackson-databind" % jacksonFasterxmlVersion force(),
  "com.fasterxml.jackson.dataformat" % "jackson-dataformat-smile" % jacksonFasterxmlVersion force(),
  "com.fasterxml.jackson.datatype" % "jackson-datatype-joda" % jacksonFasterxmlVersion force(),
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonFasterxmlVersion force()
)

libraryDependencies ++= Seq(
  "io.druid" % "druid-server" % druidVersion force(),
  "io.druid" % "druid-indexing-service" % druidVersion force(),
  "com.google.inject" % "guice" % "4.0-beta" force(),
  "com.google.inject.extensions" % "guice-servlet" % "4.0-beta" force(),
  "com.google.inject.extensions" % "guice-multibindings" % "4.0-beta" force(),
  "javax.validation" % "validation-api" % "1.1.0.Final" force()
)

//
// Storm, Chill (optional)
//

libraryDependencies ++= Seq(
  "org.apache.storm" % "storm-core" % "0.9.2-incubating-mmx1" % "optional"
    exclude("javax.jms", "jms")
    exclude("org.slf4j", "log4j-over-slf4j")
    force(),
  "com.twitter" % "chill" % "0.3.1" % "optional" cross CrossVersion.binaryMapped {
    case "2.9.1" => "2.9.3"
    case x => x
  }
)

//
// Test stuff
//

libraryDependencies <++= scalaVersion {
  case "2.9.1" => Seq(
    "org.scalatest" % "scalatest_2.9.1" % "2.0.M5b" % "test"
  )
  case "2.10.4" => Seq(
    "org.scalatest" % "scalatest_2.10" % "2.2.0" % "test"
  )
}

// Need druid-services for the test-everything integration test.
libraryDependencies ++= Seq(
  "io.druid" % "druid-services" % druidVersion % "test" force(),
  "org.apache.curator" % "curator-test" % "2.4.0" % "test" force(),
  "com.sun.jersey" % "jersey-servlet" % "1.17.1" % "test" force(),
  "junit" % "junit" % "4.11" % "test",
  "com.novocode" % "junit-interface" % "0.11-RC1" % "test"
)
