name := """ground"""
organization := "edu.berkeley"

version := "0.1-SNAPSHOT"

lazy val root = (project in file(".")).enablePlugins(PlayJava)

scalaVersion := "2.11.8"

libraryDependencies += filters
libraryDependencies += "com.datastax.cassandra" % "cassandra-driver-core" % "3.1.4"
libraryDependencies += "org.postgresql" % "postgresql" % "9.4.1208"
libraryDependencies += "org.neo4j.driver" % "neo4j-java-driver" % "1.2.0"
libraryDependencies += "org.mockito" % "mockito-core" % "2.7.22" % "test"
libraryDependencies += "org.assertj" % "assertj-core" % "3.6.2" % "test"

// disable parallel execution of tests
parallelExecution in Test := false

jacoco.settings
parallelExecution in jacoco.Config := false
