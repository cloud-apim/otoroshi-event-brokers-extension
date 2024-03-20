import Dependencies._

ThisBuild / scalaVersion     := "2.12.13"
ThisBuild / version          := "1.0.0-dev"
ThisBuild / organization     := "com.cloud-apim"
ThisBuild / organizationName := "Cloud-APIM"

lazy val excludesSlf4j = Seq(
  ExclusionRule(organization = "org.slf4j"),
)

lazy val root = (project in file("."))
  .settings(
    name := "otoroshi-extension-event-brokers",
    resolvers += "jitpack" at "https://jitpack.io",
    assembly / test  := {},
    assembly / assemblyJarName := "otoroshi-extension-event-brokers-assembly_2.12-dev.jar",
    libraryDependencies ++= Seq(
      "fr.maif" %% "otoroshi" % "16.15.3" % "provided",
      "com.rabbitmq" % "amqp-client" % "5.20.0" excludeAll(excludesSlf4j: _*),
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
