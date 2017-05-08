resolvers ++= Seq(
  "Central" at "https://oss.sonatype.org/content/repositories/releases/"
)

addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.8.2")

addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.0.4")

addSbtPlugin("com.jsuereth" % "sbt-pgp" % "1.0.0")

addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.0.5")
