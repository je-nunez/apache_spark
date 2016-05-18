resolvers ++= Seq(
  Classpaths.sbtPluginSnapshots,
  Classpaths.sbtPluginReleases
)

// We use Scala 2.10, and sbt-coverage 1.3 has an issue with 2.10:
//     https://github.com/scoverage/sbt-scoverage/issues/146

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.2.0")

addSbtPlugin("org.scoverage" % "sbt-coveralls" % "1.1.0")
