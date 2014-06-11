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
    "spy" at "http://files.couchbase.com/maven2/"
  )

  val commonDeps = Seq(
    "org.slf4j" % "slf4j-api" % "1.6.4",
    "nl.grons" %% "metrics-scala" % "2.2.0" exclude("org.slf4j", "slf4j-api"),
    "junit" % "junit" % "4.10" % "test, it",
    "org.scalatest" %% "scalatest" % "2.0" % "test, it",
    "org.mockito" % "mockito-core" % "1.9.0" % "test",
    "org.scalacheck" %% "scalacheck" % "1.10.1" % "test"
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
    "net.databinder.dispatch" %% "dispatch-lift-json" % "0.11.0" exclude("io.netty", "netty"),
    "io.netty" % "netty" % "3.5.0.Final",
    "org.json4s" %% "json4s-native" % "3.2.5",
    "com.twitter" %% "util-core" % "6.1.0",
    "org.ccil.cowan.tagsoup" % "tagsoup" % "1.2" % "test, it"
  )

  val gearmanDeps = Seq(
    "org.json4s" %% "json4s-native" % "3.2.5",
    "org.gearman" % "java-gearman-service" % "0.3"
  )

  def configureScalariform(pref: IFormattingPreferences): IFormattingPreferences = {
    pref.setPreference(AlignParameters, true)
      .setPreference(DoubleIndentClassDeclaration, true)
    }

  val defaultSettings = Defaults.defaultSettings ++ Defaults.itSettings ++ scalariformSettings ++ Seq(
    libraryDependencies ++= commonDeps,
    resolvers ++= commonResolvers,
    retrieveManaged := true,
    publishMavenStyle := true,
    organization := "com.wajam",
    version := "0.1-SNAPSHOT",
    scalaVersion := "2.10.2",
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
    .aggregate(gearman)
    .aggregate(script)

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

  lazy val gearman = Project("commons-gearman", file("commons-gearman"))
    .configs(IntegrationTest)
    .settings(defaultSettings: _*)
    .settings(libraryDependencies ++= gearmanDeps)
    .settings(testOptions in IntegrationTest := Seq(Tests.Filter(s => s.contains("Test"))))
    .settings(parallelExecution in IntegrationTest := false)
    .dependsOn(core)
    .dependsOn(tracing)

}
