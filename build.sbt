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
    // netty 4.2 moved classes around (netty-codec -> netty-codec-base): a 4.1 netty-codec next to the
    // 4.2 jars shadows them with the old signatures. play-json 3.1.0-M9 (asked by diffson) breaks
    // otoroshi's pattern matches on JsBoolean. otoroshi pins both, but its overrides do not reach us
    // through its pom. it only matters for the compile and test classpaths: none of it is assembled.
    dependencyOverrides ++= Seq(
      "io.netty" % "netty-buffer"    % nettyVersion,
      "io.netty" % "netty-codec"     % nettyVersion,
      "io.netty" % "netty-common"    % nettyVersion,
      "io.netty" % "netty-handler"   % nettyVersion,
      "io.netty" % "netty-resolver"  % nettyVersion,
      "io.netty" % "netty-transport" % nettyVersion,
      "org.playframework" %% "play-json" % playJsonVersion,
    ),
    libraryDependencies ++= Seq(
      "fr.maif" %% "otoroshi" % "18.0.0-rc3" % "provided",
      "com.rabbitmq" % "amqp-client" % "5.20.0" excludeAll(excludesSlf4j: _*),
      // shaded: it brings its own netty (with websockets and tls), which otoroshi's netty 4.2 never meets
      "com.hivemq" % "hivemq-mqtt-client-shaded" % "1.4.0" excludeAll(ExclusionRule(organization = "org.reactivestreams")),
      munit % Test,
    ) ++ testcontainers.map(_ % Test),
    // otoroshi discovers extensions and plugins by scanning the classpath reflectively, which the
    // layered test class loader defeats: the extension's own classes live in a layer the scan
    // cannot see
    Test / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat,
    // the integration suites share one otoroshi and one container per broker
    Test / parallelExecution := false,
  )
