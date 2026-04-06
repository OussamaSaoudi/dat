/*
 * DBR shim for the workload generator.
 *
 * Provides org.apache.spark.sql.delta.{DeltaLog, Snapshot} as aliases
 * to the DBR package (com.databricks.sql.transaction.tahoe).
 *
 * Build on a DBR devbox:
 *   cd shim && sbt -Dsbt.override.build.repos=true package
 *
 * Then add the jar to spark-shell:
 *   spark-shell --jars shim.jar,workload-generator-assembly.jar
 */

name := "workload-generator-dbr-shim"
version := "0.1.0"
scalaVersion := "2.13.16"

// DBR provides these on the classpath — we just need them to compile the alias
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "4.1.1" % "provided"
  // Delta is on the DBR classpath as com.databricks.sql.transaction.tahoe
  // No explicit dependency needed — it's provided by the runtime
)
