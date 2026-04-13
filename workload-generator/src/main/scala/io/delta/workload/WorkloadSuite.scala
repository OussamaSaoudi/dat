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
import java.util.concurrent.{Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.util.DynamicVariable

import org.apache.spark.sql.SparkSession

/**
 * Base class for workload generator scripts. Provides test-suite semantics:
 * each test is independent, failures are loud but don't stop execution,
 * and failed tests clean up so they auto-retry on next run.
 *
 * {{{
 * new WorkloadSuite("reads") {
 *
 *   test("basic_read", "Simple table read") {
 *     sql("CREATE TABLE tbl (id INT) USING delta")
 *     sql("INSERT INTO tbl VALUES (1),(2),(3)")
 *     val t = registerTable("tbl")
 *     read(t)
 *     snapshot(t)
 *   }
 *
 * }
 * }}}
 */
class WorkloadSuite(val suiteName: String) extends WorkloadOps {

  private case class TestDef(
      name: String,
      description: String,
      tags: Seq[String],
      body: () => Unit)

  private val tests = mutable.ArrayBuffer[TestDef]()

  /** Register a test. The body creates tables via SQL, then declares specs. */
  def test(name: String, description: String, tags: String*)(body: => Unit): Unit = {
    require(!tests.exists(_.name == name),
      s"Duplicate test name in suite '$suiteName': '$name'")
    tests += TestDef(name, description, tags, () => body)
  }

  private def runSequential(
      spark: SparkSession,
      outputDir: Path,
      force: Boolean,
      scriptContent: Option[String]): Seq[TestResult] = {
    val results = mutable.ArrayBuffer[TestResult]()

    for (td <- tests) {
      results ++= runTest(spark, td, outputDir, force, scriptContent)
    }

    printSummary(results.toSeq)
    results.toSeq
  }

  private def runParallel(
      spark: SparkSession,
      outputDir: Path,
      force: Boolean,
      parallelism: Int,
      scriptContent: Option[String]): Seq[TestResult] = {
    val executor = Executors.newFixedThreadPool(parallelism)
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(executor)

    val completedCount = new AtomicInteger(0)
    val totalTests = tests.size

    val futures = tests.map { td =>
      Future {
        val results = runTest(spark, td, outputDir, force, scriptContent)
        val count = completedCount.incrementAndGet()
        synchronized {
          println(s"  [$count/$totalTests] Completed ${td.name}")
        }
        results
      }
    }

    val allResults: Seq[TestResult] = try {
      Await.result(Future.sequence(futures.toSeq), Duration.Inf).flatten
    } finally {
      executor.shutdown()
      executor.awaitTermination(1, TimeUnit.MINUTES)
    }

    printSummary(allResults)
    allResults
  }

  private def runTest(
      spark: SparkSession,
      td: TestDef,
      outputDir: Path,
      force: Boolean,
      scriptContent: Option[String]): Seq[TestResult] = {
    val results = mutable.ArrayBuffer[TestResult]()
    val ctx = new WorkloadContext(spark, td.name, td.tags)
    try {
      WorkloadContext.withContext(ctx)(td.body())

      if (ctx.tableSpecs.isEmpty) {
        synchronized {
          System.err.println(s"  [WARN] ${td.name}: no tables declared (missing registerTable()?)")
        }
        results += TestResult(td.name, td.description, 0, passed = true,
          Seq("No tables declared"), skipped = false)
      }

      if (ctx.tableSpecs.size == 1) {
        ctx.tableSpecs.head.resolveOutputName(td.name)
      }

      for (ts <- ctx.tableSpecs) {
        val testOutputDir = outputDir.resolve(ts.outputName)

        if (!force && Files.exists(testOutputDir.resolve("table_info.json"))) {
          synchronized { println(s"  [SKIP] ${ts.outputName}") }
          results += TestResult(ts.outputName, td.description, -1, passed = true,
            Seq.empty, skipped = true)
        } else {
          val result = WorkloadGenerator.generateTable(
            spark, ts, outputDir, scriptContent, force = true)

          if (result.validationPassed) {
            synchronized {
              println(f"  [  OK] ${result.testId}%-50s ${result.specsGenerated}%d specs")
            }
            results += TestResult(result.testId, td.description,
              result.specsGenerated, passed = true, Seq.empty)
          } else {
            cleanupDir(outputDir.resolve(result.testId))
            synchronized {
              System.err.println(
                f"  [FAIL] ${result.testId}%-50s ${result.warnings.mkString("; ")}")
            }
            results += TestResult(result.testId, td.description,
              result.specsGenerated, passed = false, result.warnings)
          }
        }
      }
    } catch {
      case e: Exception =>
        synchronized {
          System.err.println(s"  [FAIL] ${td.name}: ${e.getMessage}")
          e.printStackTrace(System.err)
        }
        cleanupDir(outputDir.resolve(td.name))
        results += TestResult(td.name, td.description, 0, passed = false,
          Seq(e.getMessage))
    } finally {
      ctx.cleanup()
    }
    results.toSeq
  }

  private def printSummary(results: Seq[TestResult]): Unit = {
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
  }

  private def cleanupDir(dir: Path): Unit = {
    if (Files.exists(dir)) {
      TableCopier.cleanOutputDir(dir)
      try { Files.deleteIfExists(dir) } catch { case _: Exception => }
    }
  }

  private var _results: Seq[TestResult] = Seq.empty

  /** Results from running the suite, available after instantiation. */
  def results: Seq[TestResult] = _results

  // Init block: automatically run all tests when the suite is instantiated.
  // Reads WORKLOAD_OUTPUT_DIR (default /tmp/workloads) and WORKLOAD_FORCE from env.
  // - Passed tests keep their output (skipped on re-run).
  // - Failed tests delete their output (auto-retry on next run without --force).
  // Set WORKLOAD_PARALLEL=N to run N tests in parallel (default: 1).
  // Note: Parallel execution requires sufficient memory and may not work
  // with all Spark configurations.
  {
    def conf(key: String, default: String): String =
      sys.env.getOrElse(key, sys.props.getOrElse(key, default))
    val outputDir = Paths.get(conf("WORKLOAD_OUTPUT_DIR", "/tmp/workloads"))
    val force = conf("WORKLOAD_FORCE", "false").toBoolean
    val parallelism = conf("WORKLOAD_PARALLEL", "1").toInt
    val sourceScript = Option(conf("WORKLOAD_SOURCE_SCRIPT", null))
      .filter(_ != null).map(Paths.get(_)).filter(Files.exists(_))
    val scriptContent = sourceScript.map(p => new String(Files.readAllBytes(p), "UTF-8"))

    val spark = SparkSession.getActiveSession.getOrElse {
      SparkSession.builder()
        .master("local[*]")
        .appName("WorkloadGenerator")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
          "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.ui.enabled", "false")
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        .config("spark.databricks.delta.log.cacheSize", "0")
        .getOrCreate()
    }
    val parallelMode = parallelism > 1
    println(s"\nSuite '$suiteName': ${tests.size} tests -> $outputDir" +
      (if (parallelMode) s" (parallel: $parallelism)" else "") + "\n")

    _results = if (parallelMode) {
      runParallel(spark, outputDir, force, parallelism, scriptContent)
    } else {
      runSequential(spark, outputDir, force, scriptContent)
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
