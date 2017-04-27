name := """ground"""
organization := "edu.berkeley.ground"

version := "0.1-SNAPSHOT"

scalaVersion := "2.11.8"


lazy val root = (project in file(".")).enablePlugins(PlayJava, SwaggerPlugin)
  .settings(
    name := "ground"
  )
  .aggregate(common, postgres)


lazy val common = (project in file("modules/common"))
    .enablePlugins(PlayJava, JavaAppPackaging)
    .settings(
        name := "ground-common-lib",
        organization := "edu.berkeley.ground.lib",
		version := "0.1-SNAPSHOT",
  		scalaVersion := "2.11.8"
    )

lazy val postgres = (project in file("modules/postgres"))
    .enablePlugins(PlayJava, JavaAppPackaging, SwaggerPlugin)
    .settings(
        name := "ground-postgres",
        organization := "edu.berkeley.ground.postgres",
		version := "0.1-SNAPSHOT",
  		scalaVersion := "2.11.8",
  		libraryDependencies += javaJdbc,
  		libraryDependencies += cache,
		libraryDependencies += "org.postgresql" % "postgresql" % "42.0.0",
		libraryDependencies += "commons-beanutils" % "commons-beanutils-core" % "1.8.3"
    ).dependsOn(common)


EclipseKeys.preTasks := Seq(compile in Compile)

EclipseKeys.projectFlavor := EclipseProjectFlavor.Java           // Java project. Don't expect Scala IDE
EclipseKeys.createSrc := EclipseCreateSrc.ValueSet(EclipseCreateSrc.ManagedClasses, EclipseCreateSrc.ManagedResources)  // Use .class files instead of generated .scala files for views and routes

swaggerDomainNameSpaces := Seq("models")
