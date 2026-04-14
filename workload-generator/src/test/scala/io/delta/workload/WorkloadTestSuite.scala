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

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite

/**
 * Base class for workload generation test suites. Provides ScalaTest integration
 * with the workload generator DSL.
 *
 * {{{
 * class ReadsSuite extends WorkloadTestSuite("reads") {
 *
 *   test("basic_read") {
 *     sql("CREATE TABLE tbl (id INT) USING delta")
 *     sql("INSERT INTO tbl VALUES (1),(2),(3)")
 *     val t = registerTable("tbl")
 *     read(t)
 *     snapshot(t)
 *   }
 *
 * }
 * }}}
 *
 * Run with: sbt "testOnly *ReadsSuite"
 *
 * Environment variables:
 *   WORKLOAD_OUTPUT_DIR - Output directory (default: /tmp/workloads)
 *   WORKLOAD_FORCE - Regenerate even if output exists (default: false)
 */
abstract class WorkloadTestSuite(override val suiteName: String)
    extends AnyFunSuite
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with WorkloadOps {

  protected def outputDir: Path = Paths.get(
    sys.env.getOrElse("WORKLOAD_OUTPUT_DIR",
      sys.props.getOrElse("WORKLOAD_OUTPUT_DIR", "/tmp/workloads")))

  protected def force: Boolean =
    sys.env.getOrElse("WORKLOAD_FORCE",
      sys.props.getOrElse("WORKLOAD_FORCE", "false")).toBoolean

  @transient protected var _spark: SparkSession = _
  private var _ctx: WorkloadContext = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    _spark = SparkSession.builder()
      .master("local[*]")
      .appName(s"WorkloadGenerator-$suiteName")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.ui.enabled", "false")
      .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
      .config("spark.databricks.delta.log.cacheSize", "0")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (_spark != null) {
      _spark.stop()
      _spark = null
    }
    super.afterAll()
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
  }

  override def afterEach(): Unit = {
    if (_ctx != null) {
      _ctx.cleanup()
      _ctx = null
    }
    super.afterEach()
  }

  /**
   * Override ScalaTest's test to integrate with workload generation.
   * Each test creates tables, declares specs, then generates output.
   */
  override protected def test(testName: String, testTags: org.scalatest.Tag*)(
      testFun: => Any)(implicit pos: org.scalactic.source.Position): Unit = {
    super.test(testName, testTags: _*) {
      _ctx = new WorkloadContext(_spark, testName, Seq.empty)
      WorkloadContext.withContext(_ctx) {
        testFun

        // Skip if output exists (unless force mode)
        if (_ctx.tableSpecs.isEmpty) {
          info("No tables declared (missing registerTable()?)")
        } else {
          // Single-table tests use test name as directory
          if (_ctx.tableSpecs.size == 1) {
            _ctx.tableSpecs.head.resolveOutputName(testName)
          }

          for (ts <- _ctx.tableSpecs) {
            val testOutputDir = outputDir.resolve(ts.outputName)

            if (!force && Files.exists(testOutputDir.resolve("table_info.json"))) {
              info(s"Skipped ${ts.outputName} (exists)")
              cancel(s"${ts.outputName} already exists, use WORKLOAD_FORCE=true to regenerate")
            } else {
              val result = WorkloadGenerator.generateTable(_spark, ts, outputDir)

              if (result.validationPassed) {
                info(s"Generated ${result.specsGenerated} specs")
              } else {
                // Clean up failed output so it regenerates on retry
                cleanupDir(testOutputDir)
                fail(s"Validation failed: ${result.warnings.mkString("; ")}")
              }
            }
          }
        }
      }
    }
  }

  private def cleanupDir(dir: Path): Unit = {
    if (Files.exists(dir)) FileUtils.deleteDirectory(dir.toFile)
  }
}
