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

import scala.collection.mutable

import org.apache.spark.sql.SparkSession

/**
 * Base class for workload generator scripts. Provides test-suite semantics:
 * each test is independent, failures are loud but don't stop execution,
 * and failed tests clean up so they auto-retry on next run.
 *
 * {{{
 * new WorkloadSuite("reads") {
 *
 *   test("basic_read", "Simple table read") { w =>
 *     w.sql("CREATE TABLE tbl (id INT) USING delta")
 *     w.sql("INSERT INTO tbl VALUES (1),(2),(3)")
 *     val t = w.table("tbl")
 *     w.read(t)
 *     w.snapshot(t)
 *   }
 *
 * }.runAll()
 * }}}
 */
class WorkloadSuite(val suiteName: String) {

  private case class TestDef(
      name: String,
      description: String,
      tags: Seq[String],
      body: WorkloadContext => Unit)

  private val tests = mutable.ArrayBuffer[TestDef]()

  /** Register a test. The body creates tables via SQL, then declares specs. */
  def test(name: String, description: String, tags: String*)(
      body: WorkloadContext => Unit): Unit = {
    require(!tests.exists(_.name == name),
      s"Duplicate test name in suite '$suiteName': '$name'")
    tests += TestDef(name, description, tags, body)
  }

  /**
   * Run all registered tests.
   *
   * Reads WORKLOAD_OUTPUT_DIR (default /tmp/workloads) and WORKLOAD_FORCE from env.
   * - Passed tests keep their output (skipped on re-run).
   * - Failed tests delete their output (auto-retry on next run without --force).
   *
   * Calls System.exit — use [[run]] if you need to chain multiple suites.
   */
  def runAll(): Unit = {
    val results = run()
    val failed = results.count(!_.passed)
    System.exit(if (failed > 0) 1 else 0)
  }

  /**
   * Run all registered tests and return results without exiting.
   * Use this when running multiple suites in sequence.
   */
  def run(): Seq[TestResult] = {
    def conf(key: String, default: String): String =
      sys.env.getOrElse(key, sys.props.getOrElse(key, default))
    val outputDir = Paths.get(conf("WORKLOAD_OUTPUT_DIR", "/tmp/workloads"))
    val force = conf("WORKLOAD_FORCE", "false").toBoolean
    val sourceScript = Option(conf("WORKLOAD_SOURCE_SCRIPT", null))
      .filter(_ != null).map(Paths.get(_)).filter(Files.exists(_))
    val scriptContent = sourceScript.map(p => new String(Files.readAllBytes(p), "UTF-8"))

    val spark = SparkSession.active
    println(s"\nSuite '$suiteName': ${tests.size} tests -> $outputDir\n")

    val results = mutable.ArrayBuffer[TestResult]()

    for (td <- tests) {
      val ctx = new WorkloadContext(spark, td.name, td.tags)
      try {
        td.body(ctx)

        if (ctx.tableSpecs.isEmpty) {
          System.err.println(s"  [WARN] ${td.name}: no tables declared (missing w.table()?)")
          results += TestResult(td.name, td.description, 0, passed = true,
            Seq("No tables declared"), skipped = false)
        }

        if (ctx.tableSpecs.size == 1) {
          ctx.tableSpecs.head.resolveOutputName(td.name)
        }

        for (ts <- ctx.tableSpecs) {
          val testOutputDir = outputDir.resolve(ts.outputName)

          if (!force && Files.exists(testOutputDir.resolve("table_info.json"))) {
            println(s"  [SKIP] ${ts.outputName}")
            results += TestResult(ts.outputName, td.description, -1, passed = true,
              Seq.empty, skipped = true)
          } else {
            val result = WorkloadGenerator.generateTable(
              spark, ts, outputDir, scriptContent, force = true)

            if (result.validationPassed) {
              println(f"  [  OK] ${result.testId}%-50s ${result.specsGenerated}%d specs")
              results += TestResult(result.testId, td.description,
                result.specsGenerated, passed = true, Seq.empty)
            } else {
              // FAILED: clean up so it auto-retries next run
              cleanupDir(outputDir.resolve(result.testId))
              System.err.println(
                f"  [FAIL] ${result.testId}%-50s ${result.warnings.mkString("; ")}")
              results += TestResult(result.testId, td.description,
                result.specsGenerated, passed = false, result.warnings)
            }
          }
        }
      } catch {
        case e: Exception =>
          System.err.println(s"  [FAIL] ${td.name}: ${e.getMessage}")
          e.printStackTrace(System.err)
          cleanupDir(outputDir.resolve(td.name))
          results += TestResult(td.name, td.description, 0, passed = false,
            Seq(e.getMessage))
      } finally {
        ctx.cleanup()
      }
    }

    // Summary
    val passed = results.count(_.passed)
    val failed = results.count(!_.passed)
    val skipped = results.count(_.skipped)
    println(s"\n=== $suiteName ===")
    if (failed > 0) {
      results.filter(!_.passed).foreach { r =>
        println(s"  FAIL: ${r.testId}: ${r.errors.mkString("; ")}")
      }
    }
    println(s"$passed passed, $failed failed" +
      (if (skipped > 0) s", $skipped skipped" else ""))

    results.toSeq
  }

  private def cleanupDir(dir: Path): Unit = {
    if (Files.exists(dir)) {
      TableCopier.cleanOutputDir(dir)
      try { Files.deleteIfExists(dir) } catch { case _: Exception => }
    }
  }
}

case class TestResult(
    testId: String,
    description: String,
    specsGenerated: Int,
    passed: Boolean,
    errors: Seq[String],
    skipped: Boolean = false)
