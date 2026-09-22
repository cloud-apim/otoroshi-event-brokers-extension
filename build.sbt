import Dependencies._

ThisBuild / scalaVersion     := "3.8.4"
ThisBuild / version          := "1.0.0-dev"
ThisBuild / organization     := "com.cloud-apim"
ThisBuild / organizationName := "Cloud-APIM"

lazy val excludesSlf4j = Seq(
  ExclusionRule(organization = "org.slf4j"),
)

lazy val root = (project in file("."))
  .settings(
    name := "otoroshi-extension-event-brokers",
    scalacOptions ++= Seq(
      "-deprecation",
      "-feature",
      "-unchecked",
      "-Wunused:all",
      // the wasm4s "bundle" jar (transitive, provided) vendors an older scala 3 stdlib where
      // `scala.caps` is an object while scala-library 3.8.4 declares it as a package. otoroshi
      // itself silences the very same warning.
      "-Wconf:msg=package scala contains object and package with same name:s",
    ),
    assembly / test  := {},
    assembly / assemblyJarName := "otoroshi-extension-event-brokers-assembly_3-dev.jar",
    // otoroshi already provides the exact same scala3-library at runtime, no need to ship a
    // second copy of the whole stdlib in the plugin jar
    assembly / assemblyPackageScala / assembleArtifact := false,
    libraryDependencies ++= Seq(
      "fr.maif" %% "otoroshi" % "18.0.0-preview8" % "provided",
      "com.rabbitmq" % "amqp-client" % "5.20.0" excludeAll(excludesSlf4j: _*),
      "org.eclipse.paho" % "org.eclipse.paho.client.mqttv3" % "1.2.0",
      munit % Test
    )
  )

/*
org.slf4j:slf4j-api
io.dropwizard.metrics:metrics-core
io.micrometer:micrometer-core
io.opentelemetry:opentelemetry-api
com.fasterxml.jackson.core:jackson-databind
*/
