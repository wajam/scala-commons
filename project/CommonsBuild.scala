import sbt._
import Keys._
import com.typesafe.sbt.SbtScalariform._
import scalariform.formatter.preferences._

object CommonsBuild extends Build {
  val commonResolvers = Seq(
    // common deps
    "Wajam" at "http://ci1.cx.wajam/",
    "Sonatype" at "http://oss.sonatype.org/content/repositories/release",
    "Maven.org" at "http://repo1.maven.org/maven2",
    "Sun Maven2 Repo" at "http://download.java.net/maven/2",
    "Sun GF Maven2 Repo" at "http://download.java.net/maven/glassfish",
    "Oracle Maven2 Repo" at "http://download.oracle.com/maven",
    "Cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
    "spy" at "http://files.couchbase.com/maven2/"
  )

  val commonDeps = Seq(
    "org.slf4j" % "slf4j-api" % "1.6.4",
    "nl.grons" %% "metrics-scala" % "2.1.5" exclude("org.slf4j", "slf4j-api"),
    "junit" % "junit" % "4.10" % "test, it",
    "org.scalatest" %% "scalatest" % "2.2.0" % "test, it",
    "org.mockito" % "mockito-core" % "1.9.0" % "test, it",
    "org.scalacheck" %% "scalacheck" % "1.11.0" % "test"
  )

  val mysqlDeps = Seq(
    "mysql" % "mysql-connector-java" % "5.1.6",
    "c3p0" % "c3p0" % "0.9.1.2"
  )

  val cachingDeps = Seq(
    "spy" % "spymemcached" % "2.6"
  )

  val asyncclientDeps = Seq(
    "net.databinder.dispatch" %% "dispatch-core" % "0.11.0" exclude("io.netty", "netty"),
    "io.netty" % "netty" % "3.6.6.Final",
    "org.json4s" %% "json4s-native" % "3.2.10",
    "com.twitter" %% "util-core" % "6.20.0",
    "org.ccil.cowan.tagsoup" % "tagsoup" % "1.2" % "test, it"
  )

  lazy val hbaseDeps = Seq(
    "com.github.nscala-time" %% "nscala-time" % "1.0.0",
    "io.netty" % "netty" % "3.6.6.Final",
    "org.json4s" %% "json4s-native" % "3.2.10",
    "org.apache.hadoop" % "hadoop-client" % "2.3.0-cdh5.1.0" exclude("io.netty", "netty"),
    "org.apache.hbase" % "hbase-common" % "0.98.1-cdh5.1.0" exclude("org.slf4j", "slf4j-log4j12") exclude("io.netty", "netty") exclude("junit", "junit"),
    "org.apache.hbase" % "hbase-client" % "0.98.1-cdh5.1.0" exclude("org.slf4j", "slf4j-log4j12") exclude("io.netty", "netty") exclude("junit", "junit"),
    "com.typesafe" % "config" % "1.2.0" % "test, it"
  )

  val gearmanDeps = Seq(
    "org.json4s" %% "json4s-native" % "3.2.10",
    "org.gearman" % "java-gearman-service" % "0.3"
  )

  lazy val elasticsearchDeps = Seq(
    "org.json4s" %% "json4s-native" % "3.2.10",
    "org.elasticsearch" % "elasticsearch" % "1.4.0" exclude("io.netty", "netty")
  )

  lazy val awsDeps = Seq(
    "com.amazonaws" % "aws-java-sdk" % "1.9.7"
  )

  def configureScalariform(pref: IFormattingPreferences): IFormattingPreferences = {
    pref.setPreference(AlignParameters, true)
      .setPreference(DoubleIndentClassDeclaration, true)
    }

  val defaultSettings = Defaults.defaultSettings ++ Defaults.itSettings ++ scalariformSettingsWithIt ++ Seq(
    libraryDependencies ++= commonDeps,
    resolvers ++= commonResolvers,
    retrieveManaged := true,
    publishMavenStyle := true,
    organization := "com.wajam",
    version := "0.1-SNAPSHOT",
    scalaVersion := "2.10.2",
    crossScalaVersions := Seq("2.10.2", "2.11.1"),
    scalacOptions ++= Seq("-deprecation", "-unchecked", "-feature"),
    ScalariformKeys.preferences := configureScalariform(FormattingPreferences())
  )
  
  lazy val root = Project("commons", file("."))
    .configs(IntegrationTest)
    .settings(defaultSettings: _*)
    .settings(testOptions in IntegrationTest := Seq(Tests.Filter(s => s.contains("Test"))))
    .aggregate(core)
    .aggregate(tracing)
    .aggregate(mysql)
    .aggregate(caching)
    .aggregate(asyncclient)
    .aggregate(hbase)
    .aggregate(gearman)
    .aggregate(script)
    .aggregate(elasticsearch)
    .aggregate(aws)

  lazy val core = Project("commons-core", file("commons-core"))
    .configs(IntegrationTest)
    .settings(defaultSettings: _*)
    .settings(testOptions in IntegrationTest := Seq(Tests.Filter(s => s.contains("Test"))))
    .settings(parallelExecution in IntegrationTest := false)

  lazy val script = Project("commons-script", file("commons-script"))
    .configs(IntegrationTest)
    .settings(defaultSettings: _*)
    .settings(testOptions in IntegrationTest := Seq(Tests.Filter(s => s.contains("Test"))))
    .settings(parallelExecution in IntegrationTest := false)
    .dependsOn(core)

  lazy val tracing= Project("commons-tracing", file("commons-tracing"))
    .configs(IntegrationTest)
    .settings(defaultSettings: _*)
    .settings(testOptions in IntegrationTest := Seq(Tests.Filter(s => s.contains("Test"))))
    .settings(parallelExecution in IntegrationTest := false)
    .dependsOn(core)


  lazy val mysql = Project("commons-mysql", file("commons-mysql"))
    .configs(IntegrationTest)
    .settings(defaultSettings: _*)
    .settings(testOptions in IntegrationTest := Seq(Tests.Filter(s => s.contains("Test"))))
    .settings(parallelExecution in IntegrationTest := false)
    .settings(libraryDependencies ++= mysqlDeps)
    .dependsOn(core)
    .dependsOn(tracing)

  lazy val caching = Project("commons-caching", file("commons-caching"))
    .configs(IntegrationTest)
    .settings(defaultSettings: _*)
    .settings(libraryDependencies ++= cachingDeps)
    .settings(testOptions in IntegrationTest := Seq(Tests.Filter(s => s.contains("Test"))))
    .settings(parallelExecution in IntegrationTest := false)
    .dependsOn(core)
    .dependsOn(tracing)

  lazy val asyncclient = Project("commons-asyncclient", file("commons-asyncclient"))
    .configs(IntegrationTest)
    .settings(defaultSettings: _*)
    .settings(libraryDependencies ++= asyncclientDeps)
    .settings(testOptions in IntegrationTest := Seq(Tests.Filter(s => s.contains("Test"))))
    .settings(parallelExecution in IntegrationTest := false)
    .dependsOn(core)
    .dependsOn(tracing)

  lazy val hbase = Project("commons-hbase", file("commons-hbase"))
    .configs(IntegrationTest)
    .settings(defaultSettings: _*)
    .settings(libraryDependencies ++= hbaseDeps)
    .settings(testOptions in IntegrationTest := Seq(Tests.Filter(s => s.contains("Spec"))))
    .settings(parallelExecution in IntegrationTest := false)
    .dependsOn(core)
    .dependsOn(tracing)

  lazy val gearman = Project("commons-gearman", file("commons-gearman"))
    .configs(IntegrationTest)
    .settings(defaultSettings: _*)
    .settings(libraryDependencies ++= gearmanDeps)
    .settings(testOptions in IntegrationTest := Seq(Tests.Filter(s => s.contains("Test"))))
    .settings(parallelExecution in IntegrationTest := false)
    .dependsOn(core)
    .dependsOn(tracing)

  lazy val elasticsearch = Project("commons-elasticsearch", file("commons-elasticsearch"))
    .configs(IntegrationTest)
    .settings(defaultSettings: _*)
    .settings(libraryDependencies ++= elasticsearchDeps)
    .settings(testOptions in IntegrationTest := Seq(Tests.Filter(s => s.contains("Spec"))))
    .settings(parallelExecution in IntegrationTest := false)

  lazy val aws = Project("commons-aws", file("commons-aws"))
    .configs(IntegrationTest)
    .settings(defaultSettings: _*)
    .settings(libraryDependencies ++= awsDeps)
    .settings(testOptions in IntegrationTest := Seq(Tests.Filter(s => s.contains("Spec"))))
    .settings(parallelExecution in IntegrationTest := false)

}
