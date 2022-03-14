name := "distributed-persistence"

lazy val root =
  (project in file("."))
    .aggregate(benchmark, labs, `labs-macro`)

lazy val benchmark =
  (project in file("benchmark"))
    .enablePlugins(JmhPlugin)
    .settings(
      name := "benchmark",
      commonSettings
    )
    .dependsOn(labs)

lazy val labs =
  (project in file("labs"))
    .settings(
      name := "labs",
      commonSettings,
      libraryDependencies ++= Seq(
        "com.sparkjava"                 % "spark-core"                % "2.9.3",
        "com.squareup.okhttp3"          % "okhttp"                    % "4.9.3",
        "com.google.code.gson"          % "gson"                      % "2.8.9",
        "org.slf4j"                     % "slf4j-api"                 % "1.7.32",
        "ch.qos.logback"                % "logback-classic"           % "1.2.10",
        "co.elastic.clients"            % "elasticsearch-java"        % "7.16.3",
        "com.fasterxml.jackson.core"    % "jackson-databind"          % "2.12.3",
        "com.fasterxml.jackson.module" %% "jackson-module-scala"      % "2.13.1",
        "org.testcontainers"            % "testcontainers"            % "1.16.3",
        "org.testcontainers"            % "elasticsearch"             % "1.16.3",
        "org.testcontainers"            % "cassandra"                 % "1.16.3",
        "org.rocksdb"                   % "rocksdbjni"                % "6.28.2",
        "org.mapdb"                     % "mapdb"                     % "3.0.8",
        "com.datastax.oss"              % "java-driver-core"          % "4.5.0",
        "com.datastax.oss"              % "java-driver-query-builder" % "4.5.0",
        "com.typesafe"                  % "config"                    % "1.4.2",
        "org.scalatest"                %% "scalatest"                 % "3.2.9" % Test
      )
    )
    .dependsOn(`labs-macro`)

lazy val `labs-macro` =
  (project in file("labs-macro"))
    .settings(
      name := "labs-macro",
      commonSettings,
      libraryDependencies ++= Seq(
        "org.scala-lang" % "scala-reflect" % scalaVersion.value
      )
    )

val commonSettings =
  Def.settings(
    scalaVersion := "2.13.8"
  )
