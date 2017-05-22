import de.johoop.jacoco4sbt.XMLReport

name := """ground"""
organization := "edu.berkeley.ground"

version := "0.1"

scalaVersion := "2.11.8"

lazy val commonSettings = Seq(
  organization := "edu.berkeley.ground",
  version := "0.1",
  scalaVersion := "2.11.8"
)


lazy val root = (project in file(".")).enablePlugins(PlayJava)
  .settings(
    commonSettings,
    name := "ground"
  )
  .aggregate(common, postgres)


lazy val common = (project in file("modules/common"))
  .enablePlugins(PlayJava, JavaAppPackaging)
  .settings(
    commonSettings,
    name := "ground-common",
    libraryDependencies += javaJdbc,
    jacoco.settings,
    parallelExecution in jacoco.Config := false,
    Keys.fork in jacoco.Config := true,
    jacoco.reportFormats in jacoco.Config := Seq(XMLReport(encoding = "utf-8"))
  )

lazy val postgres = (project in file("modules/postgres"))
  .enablePlugins(PlayJava, JavaAppPackaging)
  .settings(
    commonSettings,
    name := "ground-postgres",
    libraryDependencies += javaJdbc,
    libraryDependencies += cache,
    libraryDependencies += "org.postgresql" % "postgresql" % "42.0.0",
    libraryDependencies += "commons-beanutils" % "commons-beanutils-core" % "1.8.3",
    jacoco.settings,
    parallelExecution in jacoco.Config := false,
    Keys.fork in jacoco.Config := true,
    jacoco.reportFormats in jacoco.Config := Seq(XMLReport(encoding = "utf-8"))
  ).dependsOn(common)


jacoco.settings
parallelExecution in jacoco.Config := false

jacoco.reportFormats in jacoco.Config := Seq(XMLReport(encoding = "utf-8"))
