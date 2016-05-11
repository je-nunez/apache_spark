name := "spcluster"

version := "0.0.1"

scalaVersion := "2.10.6"

fork in run := true
fork in Test := true

javaOptions in run ++= Seq(
    "-Xms4G", "-Xmx4G", "-XX:+UseG1GC"
)

testOptions in Test += Tests.Argument("-oD")

concurrentRestrictions in Global += Tags.limit(Tags.Test, 1)


resolvers += Classpaths.sbtPluginReleases

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.3.5")

addSbtPlugin("org.scoverage" % "sbt-coveralls" % "1.1.0")


lazy val apachePOIVersion = "3.14"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.1" % "provided",
  "org.apache.spark" %% "spark-mllib" % "1.6.1",
  "org.apache.commons" % "commons-lang3" % "3.0",
  "org.apache.poi" % "poi" % apachePOIVersion,
  "org.apache.poi" % "poi-ooxml" % apachePOIVersion,
  "org.apache.poi" % "poi-ooxml-schemas" % apachePOIVersion,
  "org.scalatest" %% "scalatest" % "2.2.6" % "test"
)

resolvers ++= Seq(
  "JBoss Repository" at "http://repository.jboss.org/nexus/content/repositories/releases/",
  "Spray Repository" at "http://repo.spray.cc/",
  "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
  "Akka Repository" at "http://repo.akka.io/releases/",
  "Twitter4J Repository" at "http://twitter4j.org/maven2/",
  "Apache HBase" at "https://repository.apache.org/content/repositories/releases",
  "Twitter Maven Repo" at "http://maven.twttr.com/",
  "scala-tools" at "https://oss.sonatype.org/content/groups/scala-tools",
  "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
  "Second Typesafe repo" at "http://repo.typesafe.com/typesafe/maven-releases/",
  "Mesosphere Public Repository" at "http://downloads.mesosphere.io/maven",
  Resolver.sonatypeRepo("public")
)

