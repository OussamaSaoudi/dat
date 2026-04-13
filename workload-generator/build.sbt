/*
 * Copyright (2025) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

name := "delta-workload-generator"
version := "0.1.0"
scalaVersion := "2.13.17"

// JVM module options for Java 17+ (required for Spark 4.x)
val jvmOptions = Seq(
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
  "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"
)

lazy val root = (project in file("."))
  .settings(
    name := "delta-workload-generator",
    libraryDependencies ++= Seq(
      "io.delta" %% "delta-spark" % "4.1.0" % "provided",
      "org.apache.spark" %% "spark-sql" % "4.1.0" % "provided",
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.15.2",
      "commons-io" % "commons-io" % "2.11.0",
      // Test dependencies
      "io.delta" %% "delta-spark" % "4.1.0" % "test",
      "org.apache.spark" %% "spark-sql" % "4.1.0" % "test",
      "org.scalatest" %% "scalatest" % "3.2.19" % "test",
      "org.scala-lang" % "scala-compiler" % scalaVersion.value % "test"
    ),
    Test / fork := true,
    Test / javaOptions ++= jvmOptions,
    // Include provided dependencies on the runtime classpath for runMain
    Compile / run / fork := true,
    Compile / run / javaOptions ++= jvmOptions,
    Runtime / fullClasspath ++= (Compile / managedClasspath).value.filter { jar =>
      val name = jar.data.getName
      name.contains("delta") || name.contains("spark")
    },
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", _*) => MergeStrategy.discard
      case _ => MergeStrategy.first
    }
  )
