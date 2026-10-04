ThisBuild / scalaVersion := "3.3.8"
ThisBuild / organization := "zeus"
ThisBuild / version := "0.1.0"
ThisBuild / semanticdbEnabled := true
ThisBuild / scalacOptions += "-Wunused:imports"

lazy val zioVersion = "2.1.26"
lazy val zioHttpVersion = "3.11.2"
lazy val zioJsonVersion = "0.9.1"
lazy val pureConfigVersion = "0.17.10"
lazy val zioTestDependencies = Seq(
  "dev.zio" %% "zio-test" % zioVersion % Test,
  "dev.zio" %% "zio-test-sbt" % zioVersion % Test
)

ThisBuild / testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")

lazy val config = project
  .in(file("config"))
  .settings(
    libraryDependencies ++= Seq(
      "com.github.pureconfig" %% "pureconfig-core" % pureConfigVersion
    ) ++ zioTestDependencies
  )

lazy val domain = project
  .in(file("domain"))
  .settings(
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio" % zioVersion,
      "dev.zio" %% "zio-json" % zioJsonVersion
    )
  )

lazy val redis = project
  .in(file("redis"))
  .dependsOn(domain, config)
  .settings(
    libraryDependencies ++= Seq(
      "io.lettuce" % "lettuce-core" % "7.6.0.RELEASE"
    )
  )

lazy val service = project
  .in(file("service"))
  .dependsOn(domain)
  .settings(
    libraryDependencies ++= Seq("dev.zio" %% "zio-streams" % zioVersion) ++ zioTestDependencies
  )

lazy val http = project
  .in(file("http"))
  .dependsOn(service, domain)
  .settings(libraryDependencies += "dev.zio" %% "zio-http" % zioHttpVersion)

lazy val app = project
  .in(file("app"))
  .enablePlugins(JavaAppPackaging)
  .dependsOn(http, redis, service, domain, config)
  .settings(
    libraryDependencies += "dev.zio" %% "zio-http" % zioHttpVersion,
    Compile / mainClass := Some("zeus.app.Main")
  )

lazy val zeus = project
  .in(file("."))
  .aggregate(config, domain, redis, service, http, app)
  .settings(
    name := "zeus",
    publish / skip := true
  )
