import Dependencies._

ThisBuild / scalaVersion     := "2.12.13"
ThisBuild / version          := "1.0.0-dev"
ThisBuild / organization     := "com.cloud-apim"
ThisBuild / organizationName := "Cloud-APIM"

lazy val root = (project in file("."))
  .settings(
    name := "otoroshi-extension-event-brokers",
    resolvers += "jitpack" at "https://jitpack.io",
    assembly / test  := {},
    assembly / assemblyJarName := "otoroshi-extension-event-brokers-assembly_2.12-dev.jar",
    libraryDependencies ++= Seq(
      "fr.maif" %% "otoroshi" % "16.15.1" % "provided",
      munit % Test
    )
  )
