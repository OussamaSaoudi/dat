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

import java.nio.file.{Files, Path}

import scala.jdk.CollectionConverters._

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.delta.DeltaLog

/**
 * Captures read specs with expected data and metadata.
 *
 * Exception handling: only the DataFrame construction + materialization is
 * wrapped in try/catch (read errors become error specs). Infrastructure
 * failures (writing parquet, JSON) propagate — those are real bugs.
 */
object ReadCapture {

  private val MaxExpectedDataRows = 5_000_000L

  private sealed trait ReadAttempt
  private case class ReadSuccess(
      resultDf: DataFrame, actualCount: Long,
      addFilesJson: Seq[String], totalFileCount: Long) extends ReadAttempt
  private case class ReadError(errorCode: String, errorMessage: String) extends ReadAttempt

  def capture(
      spark: SparkSession, testId: String, tablePath: Path,
      outputDir: Path, specsDir: Path, name: String,
      predicate: Option[String] = None, version: Option[Long] = None,
      timestamp: Option[String] = None, columns: Option[Seq[String]] = None): Unit = {

    val specName = s"${testId}_$name"
    require(!(version.isDefined && timestamp.isDefined),
      s"Read workload $specName cannot specify both version and timestamp")

    val expectedDir = outputDir.resolve("expected").resolve(specName)
    Files.createDirectories(expectedDir)

    val attempt: ReadAttempt = try {
      DeltaLog.clearCache()
      val deltaLog = DeltaLog.forTable(spark, tablePath.toString)
      val snapshot = (version, timestamp) match {
        case (Some(v), _) => deltaLog.getSnapshotAt(v)
        case (_, Some(ts)) =>
          val tsValue = java.sql.Timestamp.valueOf(ts)
          deltaLog.getSnapshotAt(
            deltaLog.history.getActiveCommitAtTime(
              tsValue, None, canReturnLastCommit = true,
              mustBeRecreatable = false, canReturnEarliestCommit = false).version)
        case _ => deltaLog.update()
      }

      val addFiles = predicate match {
        case Some(predicateStr) =>
          val pred = spark.sessionState.sqlParser.parseExpression(predicateStr)
          val resolved = resolvePredicateAgainstSnapshot(spark, pred, snapshot)
          snapshot.filesForScan(Seq(resolved)).files
        case None => snapshot.allFiles.collect().toSeq
      }
      val addFilesJson = addFiles.map(_.json)
      val totalFileCount = if (predicate.isEmpty) addFiles.length.toLong
        else snapshot.allFiles.count()

      var resultDf = JsonUtil.buildDeltaReader(spark, tablePath, version, timestamp)
      resultDf = JsonUtil.applyFilters(resultDf, predicate, columns)
      resultDf = resultDf.cache()
      val actualCount = resultDf.count()

      ReadSuccess(resultDf, actualCount, addFilesJson, totalFileCount)
    } catch {
      case e: Exception =>
        ReadError(JsonUtil.extractErrorCode(e), Option(e.getMessage).getOrElse(""))
    }

    attempt match {
      case ReadSuccess(resultDf, actualCount, addFilesJson, totalFileCount) =>
        try {
          writeSuccessSpec(specsDir, specName, version, timestamp, predicate, columns,
            actualCount, addFilesJson.length, totalFileCount)
          writeExpectedData(spark, expectedDir, resultDf, actualCount, specName)
          writeExpectedMetadata(spark, expectedDir, addFilesJson)
          validateCapturedRead(spark, tablePath, expectedDir, specName,
            version, timestamp, predicate, columns, addFilesJson)
          println(s"  Read spec captured: $specName ($actualCount rows, " +
            s"${addFilesJson.length}/$totalFileCount files after data skipping)")
        } finally {
          resultDf.unpersist()
        }

      case ReadError(errorCode, errorMessage) =>
        validateErrorReproducible(spark, tablePath, specName, version, timestamp,
          predicate, columns, errorCode)
        writeErrorSpec(specsDir, specName, version, timestamp, predicate, columns,
          errorCode, errorMessage)
        println(s"  Read spec captured (error): $specName [$errorCode] $errorMessage")
    }
  }

  private def writeSuccessSpec(
      specsDir: Path, specName: String,
      version: Option[Long], timestamp: Option[String],
      predicate: Option[String], columns: Option[Seq[String]],
      rowCount: Long, fileCount: Int, totalFileCount: Long): Unit = {
    val spec = buildSpecBase(version, timestamp, predicate, columns)
    val expected = new java.util.LinkedHashMap[String, Any]()
    expected.put("rowCount", rowCount)
    expected.put("fileCount", fileCount)
    expected.put("filesSkipped", (totalFileCount - fileCount))
    spec.put("expected", expected)
    JsonUtil.writeJson(specsDir.resolve(s"$specName.json"), spec)
  }

  private def writeErrorSpec(
      specsDir: Path, specName: String,
      version: Option[Long], timestamp: Option[String],
      predicate: Option[String], columns: Option[Seq[String]],
      errorCode: String, errorMessage: String): Unit = {
    val spec = buildSpecBase(version, timestamp, predicate, columns)
    val error = new java.util.LinkedHashMap[String, Any]()
    error.put("errorCode", errorCode)
    error.put("errorMessage", errorMessage)
    spec.put("error", error)
    JsonUtil.writeJson(specsDir.resolve(s"$specName.json"), spec)
  }

  private def buildSpecBase(
      version: Option[Long], timestamp: Option[String],
      predicate: Option[String], columns: Option[Seq[String]]
  ): java.util.LinkedHashMap[String, Any] = {
    val spec = new java.util.LinkedHashMap[String, Any]()
    spec.put("type", "read")
    version.foreach(v => spec.put("version", v))
    timestamp.foreach(ts => spec.put("timestamp", ts))
    predicate.foreach(p => spec.put("predicate", p))
    columns.foreach(cols => spec.put("columns", cols.asJava))
    spec
  }

  private def writeExpectedData(
      spark: SparkSession, expectedDir: Path, resultDf: DataFrame,
      actualCount: Long, specName: String): Unit = {
    val path = expectedDir.resolve("expected_data")
    if (path.toFile.exists()) FileUtils.deleteDirectory(path.toFile)
    if (actualCount <= MaxExpectedDataRows) {
      resultDf.write.mode(SaveMode.Overwrite).parquet(path.toString)
    } else {
      System.err.println(s"WARN: Skipping expected_data for $specName: " +
        s"rowCount=$actualCount exceeds threshold=$MaxExpectedDataRows")
    }
  }

  private def writeExpectedMetadata(
      spark: SparkSession, expectedDir: Path, addFilesJson: Seq[String]): Unit = {
    val path = expectedDir.resolve("expected_metadata")
    if (path.toFile.exists()) FileUtils.deleteDirectory(path.toFile)
    if (addFilesJson.nonEmpty) {
      val df = spark.createDataset(addFilesJson)(
        org.apache.spark.sql.Encoders.STRING).toDF("action")
      df.write.mode(SaveMode.Overwrite).parquet(path.toString)
    }
  }

  private def validateErrorReproducible(
      spark: SparkSession, tablePath: Path, specName: String,
      version: Option[Long], timestamp: Option[String],
      predicate: Option[String], columns: Option[Seq[String]],
      originalErrorCode: String): Unit = {
    DeltaLog.clearCache()
    val reErrorCode = try {
      val df = JsonUtil.applyFilters(
        JsonUtil.buildDeltaReader(spark, tablePath, version, timestamp),
        predicate, columns)
      df.count()
      null // succeeded — no error
    } catch {
      case e: Exception => JsonUtil.extractErrorCode(e)
    }
    if (reErrorCode == null) {
      throw new RuntimeException(
        s"Error spec validation FAILED for $specName: original failed with " +
          s"[$originalErrorCode] but re-read succeeded. Transient error.")
    }
    require(reErrorCode == originalErrorCode,
      s"Error spec validation FAILED for $specName: " +
        s"original errorCode=[$originalErrorCode] but re-read errorCode=[$reErrorCode]")
  }

  private[workload] def validateCapturedRead(
      spark: SparkSession, tablePath: Path, expectedDir: Path,
      specName: String, version: Option[Long], timestamp: Option[String],
      predicate: Option[String], columns: Option[Seq[String]],
      originalAddFilesJson: Seq[String]): Unit = {
    DeltaLog.clearCache()
    val rereadDf = JsonUtil.applyFilters(
      JsonUtil.buildDeltaReader(spark, tablePath, version, timestamp),
      predicate, columns)

    // Validate expected_data (row-level)
    val expectedDataPath = expectedDir.resolve("expected_data")
    if (Files.exists(expectedDataPath)) {
      val expectedDf = spark.read.parquet(expectedDataPath.toString)
      JsonUtil.assertMultisetsEqual(
        JsonUtil.toRowMultiset(expectedDf),
        JsonUtil.toRowMultiset(rereadDf),
        specName)
    }

    // Validate expected_metadata (AddFile actions)
    val expectedMetaPath = expectedDir.resolve("expected_metadata")
    if (Files.exists(expectedMetaPath)) {
      val expectedMetaDf = spark.read.parquet(expectedMetaPath.toString)
      val expectedActions = expectedMetaDf.collect().map(_.getString(0)).sorted
      val originalActions = originalAddFilesJson.sorted
      require(expectedActions.sameElements(originalActions),
        s"Expected metadata validation FAILED for $specName: " +
          s"written ${expectedActions.length} actions != original ${originalActions.length}")
    }
  }

  private def resolvePredicateAgainstSnapshot(
      spark: SparkSession, predicate: Expression,
      snapshot: org.apache.spark.sql.delta.Snapshot): Expression = {
    val schema = snapshot.metadata.schema
    val resolver = spark.sessionState.conf.resolver
    predicate.transform {
      case u: UnresolvedAttribute =>
        val fieldName = u.nameParts.mkString(".")
        schema.find(f => resolver(f.name, fieldName))
          .orElse(snapshot.metadata.partitionColumns
            .find(c => resolver(c, fieldName))
            .flatMap(pc => schema.find(f => resolver(f.name, pc))))
          .map(f => AttributeReference(f.name, f.dataType, f.nullable)())
          .getOrElse(u)
    }
  }
}
