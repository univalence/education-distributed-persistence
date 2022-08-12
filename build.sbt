import sbtassembly.AssemblyPlugin.autoImport.assemblyPrependShellScript
import sbtassembly.AssemblyPlugin.defaultUniversalScript

ThisBuild / scalaVersion := "2.13.8"

val libVersion =
  new {
    val cassandra      = "4.14.1"
    val elasticsearch  = "8.3.3"
    val gson           = "2.9.1"
    val jackson        = "2.13.3"
    val javaSpark      = "2.9.4"
    val kafka          = "3.2.1"
    val logback        = "1.2.11"
    val mapdb          = "3.0.8"
    val okhttp         = "4.10.0"
    val rocksdb        = "7.4.4"
    val scalatest      = "3.2.13"
    val slf4j          = "1.7.36"
    val testcontainers = "1.17.3"
    val typsafeConfig  = "1.4.2"
  }

lazy val root =
  (project in file("."))
    .disablePlugins(sbtassembly.AssemblyPlugin)
    .settings(
      name := "education-distributed-persistence"
    )
    .aggregate(
      benchmark,
      labs,
      `labs-macro`,
      `microservice-common`,
      `microservice-ingest`,
      `microservice-process`,
      `microservice-api`,
      `microservice-demo`
    )

lazy val `microservice-common` =
  (project in file("microservice/common"))
    .settings(
      name := "microservice-common",
      libraryDependencies ++= Seq(
        "com.google.code.gson" % "gson"             % libVersion.gson,
        "com.datastax.oss"     % "java-driver-core" % libVersion.cassandra
      ),
      assembly / assemblyJarName := s"${name.value}.jar"
    )

lazy val `microservice-ingest` =
  (project in file("microservice/ingest"))
    .settings(
      name := "microservice-ingest",
      libraryDependencies ++= Seq(
        "com.sparkjava"    % "spark-core"      % libVersion.javaSpark,
        "org.apache.kafka" % "kafka-clients"   % libVersion.kafka,
        "org.slf4j"        % "slf4j-api"       % libVersion.slf4j,
        "ch.qos.logback"   % "logback-classic" % libVersion.logback
      ),
      assembly / mainClass       := Some("io.univalence.microservice.ingest.MicroserviceIngestMain"),
      assembly / assemblyJarName := s"${name.value}.jar",
      assemblyPrependShellScript := Some(defaultUniversalScript(shebang = false)),
    )
    .dependsOn(`microservice-common`)

lazy val `microservice-process` =
  (project in file("microservice/process"))
    .settings(
      name := "microservice-process",
      libraryDependencies ++= Seq(
        "org.apache.kafka" % "kafka-clients"    % libVersion.kafka,
        "com.datastax.oss" % "java-driver-core" % libVersion.cassandra,
        "org.slf4j"        % "slf4j-api"        % libVersion.slf4j,
        "ch.qos.logback"   % "logback-classic"  % libVersion.logback
      ),
      assembly / mainClass       := Some("io.univalence.microservice.process.MicroserviceProcessMain"),
      assembly / assemblyJarName := s"${name.value}.jar"
    )
    .dependsOn(`microservice-common`)

lazy val `microservice-api` =
  (project in file("microservice/api"))
    .settings(
      name := "microservice-api",
      libraryDependencies ++= Seq(
        "com.sparkjava"    % "spark-core"       % libVersion.javaSpark,
        "com.datastax.oss" % "java-driver-core" % libVersion.cassandra,
        "org.slf4j"        % "slf4j-api"        % libVersion.slf4j,
        "ch.qos.logback"   % "logback-classic"  % libVersion.logback
      ),
      assembly / mainClass       := Some("io.univalence.microservice.api.MicroserviceApiMain"),
      assembly / assemblyJarName := s"${name.value}",
      assemblyPrependShellScript := Some(defaultUniversalScript(shebang = false)),
    )
    .dependsOn(`microservice-common`)

lazy val `microservice-demo` =
  (project in file("microservice/demo"))
    .disablePlugins(sbtassembly.AssemblyPlugin)
    .settings(
      name := "microservice-demo",
      libraryDependencies ++= Seq(
        "com.squareup.okhttp3" % "okhttp"           % libVersion.okhttp,
        "org.apache.kafka"     % "kafka-clients"    % libVersion.kafka,
        "com.datastax.oss"     % "java-driver-core" % libVersion.cassandra,
        "org.slf4j"            % "slf4j-api"        % libVersion.slf4j,
        "ch.qos.logback"       % "logback-classic"  % libVersion.logback
      )
    )
    .dependsOn(`microservice-common`)

lazy val benchmark =
  (project in file("benchmark"))
    .disablePlugins(sbtassembly.AssemblyPlugin)
    .enablePlugins(JmhPlugin)
    .settings(
      name := "benchmark"
    )
    .dependsOn(labs)

lazy val labs =
  (project in file("labs"))
    .disablePlugins(sbtassembly.AssemblyPlugin)
    .settings(
      name := "labs",
      libraryDependencies ++= Seq(
        "com.sparkjava"                 % "spark-core"                % libVersion.javaSpark,
        "com.squareup.okhttp3"          % "okhttp"                    % libVersion.okhttp,
        "com.google.code.gson"          % "gson"                      % libVersion.gson,
        "org.slf4j"                     % "slf4j-api"                 % libVersion.slf4j,
        "ch.qos.logback"                % "logback-classic"           % libVersion.logback,
        "co.elastic.clients"            % "elasticsearch-java"        % libVersion.elasticsearch,
        "org.apache.kafka"              % "kafka-clients"             % libVersion.kafka,
        "com.fasterxml.jackson.core"    % "jackson-databind"          % libVersion.jackson,
        "com.fasterxml.jackson.module" %% "jackson-module-scala"      % libVersion.jackson,
        "org.testcontainers"            % "testcontainers"            % libVersion.testcontainers,
        "org.testcontainers"            % "elasticsearch"             % libVersion.testcontainers,
        "org.testcontainers"            % "cassandra"                 % libVersion.testcontainers,
        "org.testcontainers"            % "kafka"                     % libVersion.testcontainers,
        "org.rocksdb"                   % "rocksdbjni"                % libVersion.rocksdb,
        "org.mapdb"                     % "mapdb"                     % libVersion.mapdb,
        "com.datastax.oss"              % "java-driver-core"          % libVersion.cassandra,
        "com.datastax.oss"              % "java-driver-query-builder" % libVersion.cassandra,
        "com.typesafe"                  % "config"                    % libVersion.typsafeConfig,
        "org.scalatest"                %% "scalatest"                 % libVersion.scalatest % Test
      )
    )
    .dependsOn(`labs-macro`)

lazy val `labs-macro` =
  (project in file("labs-macro"))
    .disablePlugins(sbtassembly.AssemblyPlugin)
    .settings(
      name := "labs-macro",
      libraryDependencies ++= Seq(
        "org.scala-lang" % "scala-reflect" % scalaVersion.value
      )
    )

ThisBuild / assemblyMergeStrategy := {
  case PathList("scala", _*) =>
    MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith "module-info.class" =>
    MergeStrategy.concat
  case x =>
    val oldStrategy = (ThisBuild / assemblyMergeStrategy).value
    oldStrategy(x)
}
Global / onChangedBuildSource := ReloadOnSourceChanges
