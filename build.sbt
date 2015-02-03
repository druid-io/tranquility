organization := "com.metamx"

name := "tranquility"

scalaVersion := "2.10.4"

lazy val root = project.in(file("."))

net.virtualvoid.sbt.graph.Plugin.graphSettings

scalacOptions := Seq("-feature", "-deprecation")

resolvers ++= Seq(
  "Metamarkets Releases" at "https://metamx.artifactoryonline.com/metamx/libs-releases/",
  "clojars" at "http://clojars.org/repo/"
)

publishMavenStyle := true

publishTo := Some("pub-libs" at "https://metamx.artifactoryonline.com/metamx/pub-libs-releases-local")

parallelExecution in Test := false

fork in Test := true

publishArtifact in (Test, packageBin) := true

// storm-core has a package and object with the same name
scalacOptions += "-Yresolve-term-conflict:object"

releaseSettings

val jacksonOneVersion = "1.9.13"
val jacksonTwoVersion = "2.4.4"
val druidVersion = "0.6.164"
val samzaVersion = "0.8.0"

libraryDependencies ++= Seq(
  "com.metamx" %% "scala-util" % "1.8.43" exclude("log4j", "log4j") force(),
  "com.metamx" % "java-util" % "0.26.6" exclude("log4j", "log4j") force()
)

libraryDependencies ++= Seq(
  "io.netty" % "netty" % "3.9.5.Final" force()
)

libraryDependencies ++= Seq(
  "org.slf4j" % "slf4j-api" % "1.7.2" force() force(),
  "org.slf4j" % "jul-to-slf4j" % "1.7.2" force() force()
)

// Curator uses Jackson 1.x internally, and older version cause problems with service discovery.
libraryDependencies ++= Seq(
  "org.codehaus.jackson" % "jackson-core-asl" % jacksonOneVersion force(),
  "org.codehaus.jackson" % "jackson-mapper-asl" % jacksonOneVersion force()
)

// We use Jackson 2.x internally (and so does Druid).
libraryDependencies ++= Seq(
  "com.fasterxml.jackson.core" % "jackson-core" % jacksonTwoVersion,
  "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonTwoVersion,
  "com.fasterxml.jackson.core" % "jackson-databind" % jacksonTwoVersion,
  "com.fasterxml.jackson.dataformat" % "jackson-dataformat-smile" % jacksonTwoVersion,
  "com.fasterxml.jackson.datatype" % "jackson-datatype-joda" % jacksonTwoVersion,
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonTwoVersion
)

libraryDependencies ++= Seq(
  "io.druid" % "druid-server" % druidVersion exclude("org.slf4j", "slf4j-log4j12") exclude("log4j", "log4j") force(),
  "io.druid" % "druid-indexing-service" % druidVersion exclude("org.slf4j", "slf4j-log4j12") exclude("log4j", "log4j") force(),
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
  "com.twitter" %% "chill" % "0.3.1" % "optional"
)

//
// Samza (optional)
//

libraryDependencies ++= Seq(
  "org.apache.samza" % "samza-api" % samzaVersion % "optional"
)

//
// Test stuff
//

libraryDependencies ++= Seq(
  "org.scalatest" % "scalatest_2.10" % "2.2.0" % "test"
)

// Need druid-services and samza-core for integration tests.
libraryDependencies ++= Seq(
  "io.druid" % "druid-services" % druidVersion % "test" exclude("org.slf4j", "slf4j-log4j12") exclude("log4j", "log4j") force(),
  "org.apache.samza" %% "samza-core" % samzaVersion % "test",
  "org.apache.curator" % "curator-test" % "2.6.0" % "test" exclude("log4j", "log4j") force(),
  "com.sun.jersey" % "jersey-servlet" % "1.17.1" % "test" force(),
  "junit" % "junit" % "4.11" % "test",
  "com.novocode" % "junit-interface" % "0.11-RC1" % "test",
  "ch.qos.logback" % "logback-core" % "1.1.2" % "test",
  "ch.qos.logback" % "logback-classic" % "1.1.2" % "test",
  "org.slf4j" % "log4j-over-slf4j" % "1.7.6" % "test",
  "org.slf4j" % "jul-to-slf4j" % "1.7.6" % "test"
)
