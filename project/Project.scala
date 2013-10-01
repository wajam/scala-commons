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
    "org.scalatest" %% "scalatest" % "1.9.2" % "test, it",
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
    "net.liftweb" %% "lift-json" % "2.5.1"
  )

  def configureScalariform(pref: IFormattingPreferences): IFormattingPreferences = {
    pref.setPreference(AlignParameters, true)
      .setPreference(DoubleIndentClassDeclaration, true)
    }

  val defaultSettings = Defaults.defaultSettings ++ Defaults.itSettings ++ Seq(
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
    .aggregate(mysql)
    .aggregate(caching)
    .aggregate(asyncclient)

  lazy val core = Project("commons-core", file("commons-core"))
    .configs(IntegrationTest)
    .settings(defaultSettings: _*)
    .settings(testOptions in IntegrationTest := Seq(Tests.Filter(s => s.contains("Test"))))
    .settings(parallelExecution in IntegrationTest := false)

  lazy val mysql = Project("commons-mysql", file("commons-mysql"))
    .configs(IntegrationTest)
    .settings(defaultSettings: _*)
    .settings(testOptions in IntegrationTest := Seq(Tests.Filter(s => s.contains("Test"))))
    .settings(parallelExecution in IntegrationTest := false)
    .settings(libraryDependencies ++= mysqlDeps)
    .dependsOn(core)

  lazy val caching = Project("commons-caching", file("commons-caching"))
    .configs(IntegrationTest)
    .settings(defaultSettings: _*)
    .settings(libraryDependencies ++= cachingDeps)
    .settings(testOptions in IntegrationTest := Seq(Tests.Filter(s => s.contains("Test"))))
    .settings(parallelExecution in IntegrationTest := false)
    .dependsOn(core)

  lazy val asyncclient = Project("commons-asyncclient", file("commons-asyncclient"))
    .configs(IntegrationTest)
    .settings(defaultSettings: _*)
    .settings(libraryDependencies ++= asyncclientDeps)
    .settings(testOptions in IntegrationTest := Seq(Tests.Filter(s => s.contains("Test"))))
    .settings(parallelExecution in IntegrationTest := false)
    .dependsOn(core)

}