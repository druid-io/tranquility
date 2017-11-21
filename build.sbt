scalaVersion in ThisBuild := "2.11.8"

// Disable parallel execution, the various Druid oriented tests need to claim ports
parallelExecution in ThisBuild := false

// Disable parallel execution, the various Druid oriented tests need to claim ports
parallelExecution in Test := false

concurrentRestrictions in Global += Tags.limitAll(1)

val jacksonOneVersion = "1.9.13"
// See https://github.com/druid-io/druid/pull/1669, https://github.com/druid-io/tranquility/pull/81 before upgrading Jackson
val jacksonTwoVersion = "2.4.6"
val jacksonTwoModuleScalaVersion = "2.4.5"
val druidVersion = "0.9.2"
val curatorVersion = "2.12.0"
val guiceVersion = "4.0"
val flinkVersion = "1.0.3"
val finagleVersion = "6.43.0"
val twitterUtilVersion = "6.42.0"
val samzaVersion = "0.12.0"
val sparkVersion = "1.6.0"
val scalatraVersion = "2.3.1"
val jettyVersion = "9.2.5.v20141112"
val apacheHttpVersion = "4.3.3"
val kafkaVersion = "0.10.1.1"
val airlineVersion = "0.7"

def dependOnDruid(artifact: String) = {
  ("io.druid" % artifact % druidVersion
    exclude("org.slf4j", "slf4j-log4j12")
    exclude("log4j", "log4j")
    exclude("org.apache.logging.log4j", "log4j-core")
    exclude("org.apache.logging.log4j", "log4j-api")
    exclude("org.apache.logging.log4j", "log4j-slf4j-impl")
    exclude("org.apache.logging.log4j", "log4j-1.2-api")
    exclude("org.apache.curator", "curator-client")
    exclude("org.apache.curator", "curator-framework")
    exclude("org.apache.curator", "curator-recipes")
    exclude("org.apache.curator", "curator-x-discovery")
    exclude("com.lmax", "disruptor") // Pulled in by log4j2, conflicts with the one Storm wants.
    exclude("com.google.code.findbugs", "annotations") // Not needed, unwanted LGPL license (see https://github.com/druid-io/druid/issues/3866)
    force())
}

val coreDependencies = Seq(
  "com.metamx" %% "scala-util" % "1.13.6"
    exclude("log4j", "log4j")
    exclude("mysql", "mysql-connector-java") // Not needed, unwanted GPLv2 license
    force(),
  "com.metamx" % "java-util" % "0.28.2" exclude("log4j", "log4j") force(),
  "io.netty" % "netty" % "3.10.5.Final" force(),
  "org.apache.curator" % "curator-client" % curatorVersion force(),
  "org.apache.curator" % "curator-framework" % curatorVersion force(),
  "org.apache.curator" % "curator-recipes" % curatorVersion force(),
  "org.apache.curator" % "curator-x-discovery" % curatorVersion force(),
  "com.twitter" %% "util-core" % twitterUtilVersion force(),
  "com.twitter" %% "finagle-core" % finagleVersion force(),
  "com.twitter" %% "finagle-http" % finagleVersion force(),
  "org.slf4j" % "slf4j-api" % "1.7.25" force() force(),
  "org.slf4j" % "jul-to-slf4j" % "1.7.25" force() force(),
  "org.apache.httpcomponents" % "httpclient" % apacheHttpVersion force(),
  "org.apache.httpcomponents" % "httpcore" % apacheHttpVersion force(),

  // Replacement for com.google.code.findbugs:annotations (see https://github.com/druid-io/druid/issues/3866)
  "com.google.code.findbugs" % "jsr305" % "2.0.1" force(),

  // Curator uses Jackson 1.x internally, and older version cause problems with service discovery.
  "org.codehaus.jackson" % "jackson-core-asl" % jacksonOneVersion force(),
  "org.codehaus.jackson" % "jackson-mapper-asl" % jacksonOneVersion force(),

  // We use Jackson 2.x internally (and so does Druid).
  "com.fasterxml.jackson.core" % "jackson-core" % jacksonTwoVersion force(),
  "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonTwoVersion force(),
  "com.fasterxml.jackson.core" % "jackson-databind" % jacksonTwoVersion force(),
  "com.fasterxml.jackson.dataformat" % "jackson-dataformat-smile" % jacksonTwoVersion force(),
  "com.fasterxml.jackson.datatype" % "jackson-datatype-joda" % jacksonTwoVersion force(),
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonTwoModuleScalaVersion force()
) ++ Seq(
  dependOnDruid("druid-server"),
  "com.google.inject" % "guice" % guiceVersion force(),
  "com.google.inject.extensions" % "guice-servlet" % guiceVersion force(),
  "com.google.inject.extensions" % "guice-multibindings" % guiceVersion force(),
  "javax.validation" % "validation-api" % "1.1.0.Final" force()
)

val loggingDependencies = Seq(
  "ch.qos.logback" % "logback-core" % "1.1.2",
  "ch.qos.logback" % "logback-classic" % "1.1.2",
  "org.apache.logging.log4j" % "log4j-to-slf4j" % "2.4",
  "org.apache.logging.log4j" % "log4j-api" % "2.4",
  "org.slf4j" % "log4j-over-slf4j" % "1.7.12",
  "org.slf4j" % "jul-to-slf4j" % "1.7.12"
)

val flinkDependencies = {
  Seq(
    "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % "optional"
    exclude("log4j", "log4j")
    exclude("org.slf4j", "slf4j-log4j12")
    force()
  )
}

val stormDependencies = Seq(
  "org.apache.storm" % "storm-core" % "0.9.3" % "optional"
    exclude("javax.jms", "jms")
    exclude("ch.qos.logback", "logback-classic")
    exclude("org.slf4j", "log4j-over-slf4j")
    force(),
  "com.twitter" %% "chill" % "0.7.1" % "optional"
)

val samzaDependencies = Seq(
  "org.apache.samza" % "samza-api" % samzaVersion % "optional"
)

val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "optional"
    exclude("org.slf4j", "log4j-over-slf4j")
    exclude("org.slf4j", "slf4j-log4j12")
    force()
)

val serverDependencies = Seq(
  "org.scalatra" %% "scalatra" % scalatraVersion,
  "org.eclipse.jetty" % "jetty-server" % jettyVersion,
  "org.eclipse.jetty" % "jetty-servlet" % jettyVersion
) ++ loggingDependencies

val kafkaDependencies = Seq(
  "org.apache.kafka" %% "kafka" % kafkaVersion
    exclude("org.slf4j", "slf4j-log4j12")
    exclude("log4j", "log4j")
    force(),
  "io.airlift" % "airline" % airlineVersion
) ++ loggingDependencies

val coreTestDependencies = Seq(
  "org.scalatest" %% "scalatest" % "2.2.5" % "test",
  dependOnDruid("druid-services") % "test",
  "org.apache.curator" % "curator-test" % curatorVersion % "test" exclude("log4j", "log4j") force(),
  "com.sun.jersey" % "jersey-servlet" % "1.17.1" % "test" force(),
  "junit" % "junit" % "4.12" % "test",
  "com.novocode" % "junit-interface" % "0.11" % "test",
  "ch.qos.logback" % "logback-core" % "1.1.2" % "test",
  "ch.qos.logback" % "logback-classic" % "1.1.2" % "test",
  "org.apache.logging.log4j" % "log4j-to-slf4j" % "2.4" % "test",
  "org.apache.logging.log4j" % "log4j-api" % "2.4" % "test",
  "org.slf4j" % "log4j-over-slf4j" % "1.7.12" % "test",
  "org.slf4j" % "jul-to-slf4j" % "1.7.12" % "test"
) ++ loggingDependencies.map(_ % "test")

val flinkTestDependencies = {
  Seq("org.apache.flink" % "flink-core" % flinkVersion % "test" classifier "tests",
    "org.apache.flink" %% "flink-runtime" % flinkVersion % "test" classifier "tests",
    "org.apache.flink" %% "flink-test-utils" % flinkVersion % "test"
  ).map(_ exclude("log4j", "log4j") exclude("org.slf4j", "slf4j-log4j12") force()) ++
    loggingDependencies.map(_ % "test")
}

val samzaTestDependencies = {
  Seq(
    "org.apache.samza" %% "samza-core" % samzaVersion % "test",
    "org.apache.samza" %% "samza-kafka" % samzaVersion % "test"
  ).map(_ exclude("log4j", "log4j") exclude("org.slf4j", "slf4j-log4j12") force()) ++
    loggingDependencies.map(_ % "test")
}

val serverTestDependencies = Seq(
  "org.scalatra" %% "scalatra-test" % scalatraVersion % "test"
)

val kafkaTestDependencies = Seq(
  "org.easymock" % "easymock" % "3.4" % "test"
)

lazy val commonSettings = Seq(
  organization := "io.druid",

  javaOptions := Seq("-Xms512m", "-Xmx512m", "-XX:MaxPermSize=256M"),

  // Target Java 7
  scalacOptions += "-target:jvm-1.7",
  javacOptions in compile ++= Seq("-source", "1.7", "-target", "1.7"),

  // resolve-term-conflict:object since storm-core has a package and object with the same name
  scalacOptions := Seq("-feature", "-deprecation", "-Yresolve-term-conflict:object"),

  licenses := Seq("Apache License, Version 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),

  homepage := Some(url("https://github.com/druid-io/tranquility")),

  publishMavenStyle := true,

  publishTo := Some("releases" at "https://oss.sonatype.org/service/local/staging/deploy/maven2/"),

  pomIncludeRepository := { _ => false },

  pomExtra := (
    <scm>
      <url>https://github.com/druid-io/tranquility.git</url>
      <connection>scm:git:git@github.com:druid-io/tranquility.git</connection>
    </scm>
      <developers>
        <developer>
          <name>Gian Merlino</name>
          <organization>Druid Project</organization>
          <organizationUrl>http://druid.io/</organizationUrl>
        </developer>
      </developers>),

  fork in Test := true
) ++ Seq(
  releasePublishArtifactsAction := PgpKeys.publishSigned.value
)

lazy val root = project.in(file("."))
  .settings(commonSettings: _*)
  .settings(publishArtifact := false)
  .aggregate(core, flink, storm, samza, spark, server, kafka)

lazy val core = project.in(file("core"))
  .settings(commonSettings: _*)
  .settings(name := "tranquility-core")
  .settings(publishArtifact in(Test, packageBin) := true)
  .settings(libraryDependencies ++= (coreDependencies ++ coreTestDependencies))

lazy val flink = project.in(file("flink"))
  .settings(commonSettings: _*)
  .settings(name := "tranquility-flink")
  .settings(libraryDependencies ++= (flinkDependencies ++ flinkTestDependencies))
  .dependsOn(core % "test->test;compile->compile")

lazy val spark = project.in(file("spark"))
  .settings(commonSettings: _*)
  .settings(name := "tranquility-spark")
  .settings(libraryDependencies ++= sparkDependencies)
  .dependsOn(core % "test->test;compile->compile")

lazy val storm = project.in(file("storm"))
  .settings(commonSettings: _*)
  .settings(name := "tranquility-storm")
  .settings(resolvers += "clojars" at "http://clojars.org/repo/")
  .settings(libraryDependencies ++= stormDependencies)
  .dependsOn(core % "test->test;compile->compile")

lazy val samza = project.in(file("samza"))
  .settings(commonSettings: _*)
  .settings(name := "tranquility-samza")
  .settings(libraryDependencies ++= (samzaDependencies ++ samzaTestDependencies))
  .settings(publishArtifact in Test := false)
  .dependsOn(core % "test->test;compile->compile")

lazy val server = project.in(file("server"))
  .settings(commonSettings: _*)
  .settings(name := "tranquility-server")
  .settings(libraryDependencies ++= (serverDependencies ++ serverTestDependencies))
  .settings(publishArtifact in Test := false)
  .dependsOn(core % "test->test;compile->compile")

lazy val kafka = project.in(file("kafka"))
  .settings(commonSettings: _*)
  .settings(name := "tranquility-kafka")
  .settings(libraryDependencies ++= (kafkaDependencies ++ kafkaTestDependencies))
  .settings(publishArtifact in Test := false)
  .dependsOn(core % "test->test;compile->compile")

lazy val distribution = project.in(file("distribution"))
  .settings(commonSettings: _*)
  .settings(name := "tranquility-distribution")
  .settings(publishArtifact in Test := false)
  .settings(mainClass in Compile := Some("com.metamx.tranquility.distribution.DistributionMain"))
  .settings(executableScriptName := "tranquility")
  .settings(bashScriptExtraDefines += """addJava "-Dlogback.configurationFile=${app_home}/../conf/logback.xml"""")
  .enablePlugins(JavaAppPackaging)
  .dependsOn(kafka, server)
