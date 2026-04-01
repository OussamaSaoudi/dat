/*
 * Copyright (2024) The Delta Lake Project Authors.
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

import java.nio.file.{Files, Path}

import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.functions._

/**
 * Captures CDF specs. Read succeeds → "expected" block. Read fails → "error" block.
 * NOTE: CDF type is not yet supported by the Rust harness (TODO). Specs are still
 * generated for future consumption.
 *
 * [C2] Narrow try/catch: only wraps the CDF DataFrame construction + materialization.
 */
object CdfCapture {

  private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)

  private sealed trait CdfAttempt
  private case class CdfSuccess(df: DataFrame, count: Long, latestVersion: Long) extends CdfAttempt
  private case class CdfError(errorCode: String, errorMessage: String) extends CdfAttempt

  def capture(
      spark: SparkSession,
      testId: String,
      tablePath: Path,
      outputDir: Path,
      specsDir: Path,
      name: String,
      startVersion: Option[Long] = None,
      endVersion: Option[Long] = None,
      startTimestamp: Option[String] = None,
      endTimestamp: Option[String] = None,
      predicate: Option[String] = None,
      columns: Option[Seq[String]] = None): Unit = {

    val specName = s"${testId}_$name"
    validateBounds(specName, startVersion, endVersion, startTimestamp, endTimestamp)

    val expectedDir = outputDir.resolve("expected").resolve(specName)
    Files.createDirectories(expectedDir)

    // [C2] NARROW try/catch: only wraps CDF read
    val attempt: CdfAttempt = try {
      val deltaLog = DeltaLog.forTable(spark, tablePath.toString)
      DeltaLog.clearCache()
      val latestVersion = deltaLog.update().version

      var reader = spark.read.format("delta").option("readChangeFeed", "true")
      startVersion.foreach(v => reader = reader.option("startingVersion", v))
      startTimestamp.foreach(ts => reader = reader.option("startingTimestamp", ts))
      endVersion.foreach(v => reader = reader.option("endingVersion", v))
      endTimestamp.foreach(ts => reader = reader.option("endingTimestamp", ts))
      if (startVersion.isDefined && endVersion.isEmpty && endTimestamp.isEmpty) {
        reader = reader.option("endingVersion", latestVersion)
      }

      var df = reader.load(tablePath.toString)
      predicate.foreach(p => df = df.filter(p))
      columns.foreach(cols => df = df.select(cols.map(columnRef): _*))

      df = df.cache()
      val count = df.count()
      CdfSuccess(df, count, latestVersion)
    } catch {
      case e: Exception =>
        val errorCode = e match {
          case st: org.apache.spark.SparkThrowable =>
            Option(st.getErrorClass).getOrElse(e.getClass.getSimpleName)
          case _ => e.getClass.getSimpleName
        }
        CdfError(errorCode, Option(e.getMessage).getOrElse(""))
    }

    // Infrastructure — failures propagate
    attempt match {
      case CdfSuccess(df, count, latestVersion) =>
        try {
          val expectedDataPath = expectedDir.resolve("expected_data")
          if (expectedDataPath.toFile.exists()) FileUtils.deleteDirectory(expectedDataPath.toFile)
          df.write.mode(SaveMode.Overwrite).parquet(expectedDataPath.toString)

          val summary = new java.util.LinkedHashMap[String, Any]()
          summary.put("actual_row_count", count.asInstanceOf[AnyRef])
          JsonUtil.writeJson(expectedDir.resolve("summary.json"), summary)

          val spec = new java.util.LinkedHashMap[String, Any]()
          spec.put("type", "cdf")
          startVersion.foreach(v => spec.put("startVersion", v.asInstanceOf[AnyRef]))
          startTimestamp.foreach(ts => spec.put("startTimestamp", ts))
          endVersion.foreach(v => spec.put("endVersion", v.asInstanceOf[AnyRef]))
          endTimestamp.foreach(ts => spec.put("endTimestamp", ts))
          predicate.foreach(p => spec.put("predicate", p))
          columns.foreach(cols => spec.put("columns", cols.asJava))
          val expected = new java.util.LinkedHashMap[String, Any]()
          expected.put("rowCount", count.asInstanceOf[AnyRef])
          spec.put("expected", expected)
          JsonUtil.writeJson(specsDir.resolve(s"$specName.json"), spec)

          // Post-capture validation
          validateCapturedCdf(spark, tablePath, expectedDir, specName,
            startVersion, endVersion, startTimestamp, endTimestamp,
            predicate, columns, count)

          val startStr = startVersion.map(v => s"v$v")
            .orElse(startTimestamp.map(ts => s"ts($ts)")).get
          val endStr = endVersion.map(v => s"v$v")
            .orElse(endTimestamp.map(ts => s"ts($ts)"))
            .getOrElse(s"v$latestVersion")
          println(s"  CDF spec captured: $specName ($count rows, $startStr->$endStr)")
        } finally {
          df.unpersist()
        }

      case CdfError(errorCode, errorMessage) =>
        // Re-validate: confirm error is reproducible on copied table
        validateErrorReproducible(spark, tablePath, specName,
          startVersion, endVersion, startTimestamp, endTimestamp,
          predicate, columns, errorCode)

        val spec = new java.util.LinkedHashMap[String, Any]()
        spec.put("type", "cdf")
        startVersion.foreach(v => spec.put("startVersion", v.asInstanceOf[AnyRef]))
        startTimestamp.foreach(ts => spec.put("startTimestamp", ts))
        endVersion.foreach(v => spec.put("endVersion", v.asInstanceOf[AnyRef]))
        endTimestamp.foreach(ts => spec.put("endTimestamp", ts))
        predicate.foreach(p => spec.put("predicate", p))
        columns.foreach(cols => spec.put("columns", cols.asJava))
        val error = new java.util.LinkedHashMap[String, Any]()
        error.put("errorCode", errorCode)
        error.put("errorMessage", errorMessage)
        spec.put("error", error)
        JsonUtil.writeJson(specsDir.resolve(s"$specName.json"), spec)
        println(s"  CDF spec captured (error): $specName [$errorCode] $errorMessage")
    }
  }

  /** Confirm a CDF error is reproducible on the copied table. */
  private def validateErrorReproducible(
      spark: SparkSession,
      tablePath: Path,
      specName: String,
      startVersion: Option[Long],
      endVersion: Option[Long],
      startTimestamp: Option[String],
      endTimestamp: Option[String],
      predicate: Option[String],
      columns: Option[Seq[String]],
      originalErrorCode: String): Unit = {
    DeltaLog.clearCache()
    val reAttemptSucceeded = try {
      val deltaLog = DeltaLog.forTable(spark, tablePath.toString)
      val latestVersion = deltaLog.update().version
      var reader = spark.read.format("delta").option("readChangeFeed", "true")
      startVersion.foreach(v => reader = reader.option("startingVersion", v))
      startTimestamp.foreach(ts => reader = reader.option("startingTimestamp", ts))
      endVersion.foreach(v => reader = reader.option("endingVersion", v))
      endTimestamp.foreach(ts => reader = reader.option("endingTimestamp", ts))
      if (startVersion.isDefined && endVersion.isEmpty && endTimestamp.isEmpty) {
        reader = reader.option("endingVersion", latestVersion)
      }
      var df = reader.load(tablePath.toString)
      predicate.foreach(p => df = df.filter(p))
      columns.foreach(cols => df = df.select(cols.map(columnRef): _*))
      df.count()
      true
    } catch {
      case _: Exception => false
    }
    if (reAttemptSucceeded) {
      throw new RuntimeException(
        s"CDF error spec validation FAILED for $specName: original read failed with " +
          s"[$originalErrorCode] but re-read against copied table succeeded.")
    }
  }

  /**
   * Re-execute CDF read against the copied table and validate row-for-row
   * against expected_data parquet using canonical JSON multiset comparison.
   */
  private def validateCapturedCdf(
      spark: SparkSession,
      tablePath: Path,
      expectedDir: Path,
      specName: String,
      startVersion: Option[Long],
      endVersion: Option[Long],
      startTimestamp: Option[String],
      endTimestamp: Option[String],
      predicate: Option[String],
      columns: Option[Seq[String]],
      expectedCount: Long): Unit = {
    DeltaLog.clearCache()
    val deltaLog = DeltaLog.forTable(spark, tablePath.toString)
    val latestVersion = deltaLog.update().version

    var reader = spark.read.format("delta").option("readChangeFeed", "true")
    startVersion.foreach(v => reader = reader.option("startingVersion", v))
    startTimestamp.foreach(ts => reader = reader.option("startingTimestamp", ts))
    endVersion.foreach(v => reader = reader.option("endingVersion", v))
    endTimestamp.foreach(ts => reader = reader.option("endingTimestamp", ts))
    if (startVersion.isDefined && endVersion.isEmpty && endTimestamp.isEmpty) {
      reader = reader.option("endingVersion", latestVersion)
    }

    var rereadDf = reader.load(tablePath.toString)
    predicate.foreach(p => rereadDf = rereadDf.filter(p))
    columns.foreach(cols => rereadDf = rereadDf.select(cols.map(columnRef): _*))

    // Full row-level comparison against expected_data parquet
    val expectedDataPath = expectedDir.resolve("expected_data")
    if (Files.exists(expectedDataPath)) {
      val expectedDf = spark.read.parquet(expectedDataPath.toString)
      val rereadMultiset = toRowMultiset(rereadDf)
      val expectedMultiset = toRowMultiset(expectedDf)

      if (rereadMultiset != expectedMultiset) {
        val missing = expectedMultiset.keySet -- rereadMultiset.keySet
        val extra = rereadMultiset.keySet -- expectedMultiset.keySet
        val details = new StringBuilder()
        if (missing.nonEmpty) {
          details.append(s"\n  Missing rows: ${missing.size}")
          missing.take(3).foreach(r => details.append(s"\n    $r"))
        }
        if (extra.nonEmpty) {
          details.append(s"\n  Extra rows: ${extra.size}")
          extra.take(3).foreach(r => details.append(s"\n    $r"))
        }
        throw new RuntimeException(
          s"Post-capture validation FAILED for $specName: " +
            s"row-level CDF data mismatch (expected ${expectedMultiset.values.sum} rows, " +
            s"got ${rereadMultiset.values.sum} rows)$details")
      }
    } else {
      // Fallback: count-only check if expected_data doesn't exist
      val rereadCount = rereadDf.count()
      require(rereadCount == expectedCount,
        s"Post-capture validation FAILED for $specName: " +
          s"re-read count=$rereadCount != captured count=$expectedCount")
    }
  }

  private def toRowMultiset(df: DataFrame): Map[String, Int] = {
    df.toJSON.collect().toSeq.groupBy(identity).map {
      case (value, rows) => value -> rows.size
    }
  }

  private def validateBounds(
      specName: String,
      startVersion: Option[Long], endVersion: Option[Long],
      startTimestamp: Option[String], endTimestamp: Option[String]): Unit = {
    require(startVersion.isDefined || startTimestamp.isDefined,
      s"CDF workload $specName: exactly one lower bound required")
    require(!(startVersion.isDefined && startTimestamp.isDefined),
      s"CDF workload $specName: cannot specify both startVersion and startTimestamp")
    require(!(endVersion.isDefined && endTimestamp.isDefined),
      s"CDF workload $specName: cannot specify both endVersion and endTimestamp")
  }

  private def columnRef(name: String): Column = {
    if (name.startsWith("_metadata.")) col(name)
    else { val escaped = name.replace("`", "``"); col(s"`$escaped`") }
  }
}
