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

import scala.jdk.CollectionConverters._

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.delta.DeltaLog

/** Captures CDF specs. Failed reads become error specs automatically. */
object CdfCapture {

  private sealed trait CdfAttempt
  private case class CdfSuccess(df: DataFrame, count: Long, latestVersion: Long) extends CdfAttempt
  private case class CdfError(errorCode: String, errorMessage: String) extends CdfAttempt

  def capture(
      spark: SparkSession, testId: String, tablePath: Path,
      outputDir: Path, specsDir: Path, name: String,
      startVersion: Option[Long] = None, endVersion: Option[Long] = None,
      startTimestamp: Option[String] = None, endTimestamp: Option[String] = None,
      predicate: Option[String] = None, columns: Option[Seq[String]] = None): Unit = {

    val specName = s"${testId}_$name"
    validateBounds(specName, startVersion, endVersion, startTimestamp, endTimestamp)

    val expectedDir = outputDir.resolve("expected").resolve(specName)
    Files.createDirectories(expectedDir)

    val attempt: CdfAttempt = try {
      DeltaLog.clearCache()
      val latestVersion = DeltaLog.forTable(spark, tablePath.toString).update().version
      var df = buildCdfReader(spark, tablePath, startVersion, endVersion,
        startTimestamp, endTimestamp, latestVersion)
      df = JsonUtil.applyFilters(df, predicate, columns)
      df = df.cache()
      CdfSuccess(df, df.count(), latestVersion)
    } catch {
      case e: Exception =>
        CdfError(JsonUtil.extractErrorCode(e), Option(e.getMessage).getOrElse(""))
    }

    attempt match {
      case CdfSuccess(df, count, latestVersion) =>
        try {
          val expectedDataPath = expectedDir.resolve("expected_data")
          if (expectedDataPath.toFile.exists()) FileUtils.deleteDirectory(expectedDataPath.toFile)
          df.write.mode(SaveMode.Overwrite).parquet(expectedDataPath.toString)

          val spec = buildCdfSpecBase(startVersion, endVersion, startTimestamp, endTimestamp,
            predicate, columns)
          val expected = new java.util.LinkedHashMap[String, Any]()
          expected.put("rowCount", count)
          spec.put("expected", expected)
          JsonUtil.writeJson(specsDir.resolve(s"$specName.json"), spec)

          validateCapturedCdf(spark, tablePath, expectedDir, specName,
            startVersion, endVersion, startTimestamp, endTimestamp,
            predicate, columns, count)

          val startStr = startVersion.map(v => s"v$v")
            .orElse(startTimestamp.map(ts => s"ts($ts)")).getOrElse("?")
          val endStr = endVersion.map(v => s"v$v")
            .orElse(endTimestamp.map(ts => s"ts($ts)"))
            .getOrElse(s"v$latestVersion")
          println(s"  CDF spec captured: $specName ($count rows, $startStr->$endStr)")
        } finally {
          df.unpersist()
        }

      case CdfError(errorCode, errorMessage) =>
        validateErrorReproducible(spark, tablePath, specName,
          startVersion, endVersion, startTimestamp, endTimestamp,
          predicate, columns, errorCode)
        val spec = buildCdfSpecBase(startVersion, endVersion, startTimestamp, endTimestamp,
          predicate, columns)
        val error = new java.util.LinkedHashMap[String, Any]()
        error.put("errorCode", errorCode)
        error.put("errorMessage", errorMessage)
        spec.put("error", error)
        JsonUtil.writeJson(specsDir.resolve(s"$specName.json"), spec)
        println(s"  CDF spec captured (error): $specName [$errorCode] $errorMessage")
    }
  }

  private def buildCdfSpecBase(
      startVersion: Option[Long], endVersion: Option[Long],
      startTimestamp: Option[String], endTimestamp: Option[String],
      predicate: Option[String], columns: Option[Seq[String]]
  ): java.util.LinkedHashMap[String, Any] = {
    val spec = new java.util.LinkedHashMap[String, Any]()
    spec.put("type", "cdf")
    startVersion.foreach(v => spec.put("startVersion", v))
    startTimestamp.foreach(ts => spec.put("startTimestamp", ts))
    endVersion.foreach(v => spec.put("endVersion", v))
    endTimestamp.foreach(ts => spec.put("endTimestamp", ts))
    predicate.foreach(p => spec.put("predicate", p))
    columns.foreach(cols => spec.put("columns", cols.asJava))
    spec
  }

  private def buildCdfReader(
      spark: SparkSession, tablePath: Path,
      startVersion: Option[Long], endVersion: Option[Long],
      startTimestamp: Option[String], endTimestamp: Option[String],
      latestVersion: Long): DataFrame = {
    var reader = spark.read.format("delta").option("readChangeFeed", "true")
    startVersion.foreach(v => reader = reader.option("startingVersion", v))
    startTimestamp.foreach(ts => reader = reader.option("startingTimestamp", ts))
    endVersion.foreach(v => reader = reader.option("endingVersion", v))
    endTimestamp.foreach(ts => reader = reader.option("endingTimestamp", ts))
    if (startVersion.isDefined && endVersion.isEmpty && endTimestamp.isEmpty) {
      reader = reader.option("endingVersion", latestVersion)
    }
    reader.load(tablePath.toString)
  }

  private def validateErrorReproducible(
      spark: SparkSession, tablePath: Path, specName: String,
      startVersion: Option[Long], endVersion: Option[Long],
      startTimestamp: Option[String], endTimestamp: Option[String],
      predicate: Option[String], columns: Option[Seq[String]],
      originalErrorCode: String): Unit = {
    DeltaLog.clearCache()
    val reErrorCode = try {
      val latestVersion = DeltaLog.forTable(spark, tablePath.toString).update().version
      var df = buildCdfReader(spark, tablePath, startVersion, endVersion,
        startTimestamp, endTimestamp, latestVersion)
      df = JsonUtil.applyFilters(df, predicate, columns)
      df.count()
      null // succeeded
    } catch {
      case e: Exception => JsonUtil.extractErrorCode(e)
    }
    if (reErrorCode == null) {
      throw new RuntimeException(
        s"CDF error spec validation FAILED for $specName: original failed with " +
          s"[$originalErrorCode] but re-read succeeded.")
    }
    require(reErrorCode == originalErrorCode,
      s"CDF error spec validation FAILED for $specName: " +
        s"original errorCode=[$originalErrorCode] but re-read errorCode=[$reErrorCode]")
  }

  private def validateCapturedCdf(
      spark: SparkSession, tablePath: Path, expectedDir: Path, specName: String,
      startVersion: Option[Long], endVersion: Option[Long],
      startTimestamp: Option[String], endTimestamp: Option[String],
      predicate: Option[String], columns: Option[Seq[String]],
      expectedCount: Long): Unit = {
    DeltaLog.clearCache()
    val latestVersion = DeltaLog.forTable(spark, tablePath.toString).update().version
    var rereadDf = buildCdfReader(spark, tablePath, startVersion, endVersion,
      startTimestamp, endTimestamp, latestVersion)
    rereadDf = JsonUtil.applyFilters(rereadDf, predicate, columns)

    val expectedDataPath = expectedDir.resolve("expected_data")
    if (Files.exists(expectedDataPath)) {
      val expectedDf = spark.read.parquet(expectedDataPath.toString)
      JsonUtil.assertMultisetsEqual(
        JsonUtil.toRowMultiset(expectedDf),
        JsonUtil.toRowMultiset(rereadDf),
        specName)
    } else {
      val rereadCount = rereadDf.count()
      require(rereadCount == expectedCount,
        s"Validation FAILED for $specName: count=$rereadCount != expected=$expectedCount")
    }
  }

  private def validateBounds(
      specName: String, startVersion: Option[Long], endVersion: Option[Long],
      startTimestamp: Option[String], endTimestamp: Option[String]): Unit = {
    require(startVersion.isDefined || startTimestamp.isDefined,
      s"CDF $specName: one lower bound required")
    require(!(startVersion.isDefined && startTimestamp.isDefined),
      s"CDF $specName: cannot specify both startVersion and startTimestamp")
    require(!(endVersion.isDefined && endTimestamp.isDefined),
      s"CDF $specName: cannot specify both endVersion and endTimestamp")
  }
}
