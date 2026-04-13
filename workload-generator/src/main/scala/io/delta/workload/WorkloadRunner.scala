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

package io.delta.workload

import java.nio.file.Files

import org.apache.spark.sql.SparkSession

/**
 * Main entry point for running sample workload generation via sbt.
 *
 * Usage:
 *   sbt "Test/runMain io.delta.workload.WorkloadRunner"
 *
 * For running table definition scripts, use TableScriptRunner.
 */
object WorkloadRunner {

  def main(args: Array[String]): Unit = {
    val outputDir = sys.env.getOrElse("WORKLOAD_OUTPUT_DIR", "/tmp/workloads")
    val force = sys.env.getOrElse("WORKLOAD_FORCE", "true").toBoolean

    val warehouseDir = Files.createTempDirectory("workload-warehouse-")
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("WorkloadGenerator")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.warehouse.dir", warehouseDir.toString)
      .config("spark.ui.enabled", "false")
      .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
      .config("spark.databricks.delta.log.cacheSize", "0")
      .getOrCreate()

    try {
      println(s"Output: $outputDir")
      println(s"Force: $force")
      println()

      sys.props("WORKLOAD_OUTPUT_DIR") = outputDir
      sys.props("WORKLOAD_FORCE") = force.toString

      val suite = new WorkloadSuite("sample") {
        test("basic_reads", "Basic table reads") {
          sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
          sql("INSERT INTO tbl VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")
          val t = registerTable("tbl")
          read(t)
          read(t, predicate = "id > 1")
          read(t, columns = Seq("name"))
          snapshot(t)
        }

        test("time_travel", "Time travel reads") {
          sql("CREATE TABLE tt_tbl (id INT) USING delta")
          sql("INSERT INTO tt_tbl VALUES (1)")
          sql("INSERT INTO tt_tbl VALUES (2)")
          sql("INSERT INTO tt_tbl VALUES (3)")
          val t = registerTable("tt_tbl")
          read(t, version = 1)
          read(t, version = 2)
          read(t)
          for (v <- 0L to 3) snapshot(t, version = v)
        }

        test("write_ops", "Write operations") {
          sql("CREATE TABLE write_tbl (id INT, val STRING) USING delta")
          sql("INSERT INTO write_tbl VALUES (1, 'a'), (2, 'b')")
          sql("UPDATE write_tbl SET val = 'updated' WHERE id = 1")
          sql("DELETE FROM write_tbl WHERE id = 2")
          val t = registerTable("write_tbl")
          read(t, version = 1)
          read(t, version = 2)
          read(t)
          for (v <- 0L to 3) snapshot(t, version = v)
        }
      }

      val results = suite.results

      println()
      println("=== Results ===")
      var passed = 0
      var failed = 0
      for (r <- results) {
        if (r.passed) {
          println(s"  [OK] ${r.testId}")
          passed += 1
        } else {
          println(s"  [FAIL] ${r.testId}: ${r.errors.mkString("; ")}")
          failed += 1
        }
      }
      println()
      println(s"$passed passed, $failed failed")

      if (failed > 0) System.exit(1)
    } finally {
      spark.stop()
      org.apache.commons.io.FileUtils.deleteDirectory(warehouseDir.toFile)
    }
  }
}
