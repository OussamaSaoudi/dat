/*
 * Project template for workload generation scripts with full IDE support.
 *
 * Copy this directory, put your scripts in tables/, open in IntelliJ or Metals.
 * Full autocomplete, cmd+click, compile-time checking.
 *
 * To run:
 *   ../bin/generate-workload.sh tables/my_tables.scala
 */

name := "my-workloads"
version := "0.1.0"
scalaVersion := "2.13.16"

libraryDependencies ++= Seq(
  "io.delta" %% "delta-spark" % "3.3.2" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.5.0" % "provided"
)

// Point to the workload-generator jar (adjust path as needed)
Compile / unmanagedJars += {
  val jar = baseDirectory.value / ".." / "target" / "scala-2.13" /
    "delta-workload-generator-assembly-0.1.0.jar"
  Attributed.blank(jar)
}
