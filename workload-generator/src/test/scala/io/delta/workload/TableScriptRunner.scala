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

import java.nio.file.{Files, Path, Paths}

import scala.jdk.CollectionConverters._
import scala.reflect.runtime.universe
import scala.tools.reflect.ToolBox

import org.apache.spark.sql.SparkSession

/**
 * Runs table definition scripts from the tables/ directory.
 *
 * Usage:
 *   sbt "Test/runMain io.delta.workload.TableScriptRunner"                    # Run all scripts
 *   sbt "Test/runMain io.delta.workload.TableScriptRunner tables/reads.scala" # Run one script
 *   sbt "Test/runMain io.delta.workload.TableScriptRunner tables/"            # Run directory
 */
object TableScriptRunner {

  def main(args: Array[String]): Unit = {
    val outputDir = sys.env.getOrElse("WORKLOAD_OUTPUT_DIR", "/tmp/workloads")
    val force = sys.env.getOrElse("WORKLOAD_FORCE", "true").toBoolean

    val warehouseDir = Files.createTempDirectory("workload-warehouse-")
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("TableScriptRunner")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.warehouse.dir", warehouseDir.toString)
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    try {
      println(s"Output: $outputDir")
      println(s"Force: $force")
      println()

      sys.props("WORKLOAD_OUTPUT_DIR") = outputDir
      sys.props("WORKLOAD_FORCE") = force.toString
      sys.props("WORKLOAD_NO_EXIT") = "true"

      // Determine scripts to run
      val tablesDir = Paths.get("tables")
      val scripts = if (args.isEmpty) {
        // Run all scripts in tables/
        if (Files.isDirectory(tablesDir)) {
          Files.list(tablesDir).iterator().asScala
            .filter(_.toString.endsWith(".scala"))
            .toSeq.sortBy(_.toString)
        } else {
          System.err.println(s"ERROR: tables/ directory not found")
          System.exit(1)
          Seq.empty[Path]
        }
      } else {
        val input = Paths.get(args(0))
        if (Files.isDirectory(input)) {
          Files.list(input).iterator().asScala
            .filter(_.toString.endsWith(".scala"))
            .toSeq.sortBy(_.toString)
        } else if (Files.exists(input)) {
          Seq(input)
        } else {
          System.err.println(s"ERROR: ${args(0)} not found")
          System.exit(1)
          Seq.empty[Path]
        }
      }

      println(s"Scripts to run: ${scripts.size}")
      scripts.foreach(s => println(s"  - ${s.getFileName}"))
      println()

      var passed = 0
      var failed = 0
      val tb = universe.runtimeMirror(getClass.getClassLoader).mkToolBox()

      for (script <- scripts) {
        println(s"=== Running ${script.getFileName} ===")
        try {
          val code = new String(Files.readAllBytes(script), "UTF-8")
          val wrappedCode = s"""
            |import io.delta.workload._
            |
            |$code
          """.stripMargin

          tb.eval(tb.parse(wrappedCode))
          println(s"[OK] ${script.getFileName}")
          passed += 1
        } catch {
          case e: Throwable =>
            System.err.println(s"[FAIL] ${script.getFileName}: ${e.getMessage}")
            if (sys.env.getOrElse("DEBUG", "false") == "true") {
              e.printStackTrace()
            }
            failed += 1
        }
        println()
      }

      println("=== Summary ===")
      println(s"$passed passed, $failed failed")

      if (failed > 0) System.exit(1)
    } finally {
      spark.stop()
      org.apache.commons.io.FileUtils.deleteDirectory(warehouseDir.toFile)
    }
  }
}
