import sbt._

object Dependencies {
  lazy val munit = "org.scalameta" %% "munit" % "1.0.4"

  // the version otoroshi runs on: transitive deps of the provided jar still ask for 4.1.x and play-json
  // 3.1.0-M9, and dependencyOverrides do not travel through otoroshi's pom
  lazy val nettyVersion    = "4.2.17.Final"
  lazy val playJsonVersion = "3.0.6"

  lazy val testcontainersVersion = "2.0.5"
  lazy val testcontainers = Seq(
    "org.testcontainers" % "testcontainers"          % testcontainersVersion,
    "org.testcontainers" % "testcontainers-kafka"    % testcontainersVersion,
    "org.testcontainers" % "testcontainers-rabbitmq" % testcontainersVersion,
    "org.testcontainers" % "testcontainers-pulsar"   % testcontainersVersion,
  )
}
