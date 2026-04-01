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
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, BinaryType, MapType, NullType, StructType}

/**
 * Captures read specs with expected data and metadata.
 *
 * If the read succeeds → spec with "expected" block + parquet data.
 * If the read fails → spec with "error" block (errorCode + errorMessage).
 *
 * [C2] The try/catch is NARROW: only wraps the DataFrame construction + materialization.
 * Infrastructure operations (writing parquet, serializing JSON) are OUTSIDE the catch
 * and propagate on failure — those are real bugs, not expected errors.
 */
object ReadCapture {

  private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)
  private val MaxExpectedDataRows = 5_000_000L

  /** Result of attempting a read — either success data or an error. */
  private sealed trait ReadAttempt
  private case class ReadSuccess(
      resultDf: DataFrame,
      actualCount: Long,
      addFiles: Seq[org.apache.spark.sql.delta.actions.AddFile],
      totalFileCount: Long
  ) extends ReadAttempt
  private case class ReadError(errorCode: String, errorMessage: String) extends ReadAttempt

  def capture(
      spark: SparkSession,
      testId: String,
      tablePath: Path,
      outputDir: Path,
      specsDir: Path,
      name: String,
      predicate: Option[String] = None,
      version: Option[Long] = None,
      timestamp: Option[String] = None,
      columns: Option[Seq[String]] = None): Unit = {

    val specName = s"${testId}_$name"
    require(!(version.isDefined && timestamp.isDefined),
      s"Read workload $specName cannot specify both version and timestamp")

    val expectedDir = outputDir.resolve("expected").resolve(specName)
    Files.createDirectories(expectedDir)

    // [C2] NARROW try/catch: only wraps the read attempt (DataFrame construction + count).
    // If the read fails, we capture an error spec. If infrastructure fails, we propagate.
    val attempt: ReadAttempt = try {
      val deltaLog = DeltaLog.forTable(spark, tablePath.toString)
      DeltaLog.clearCache()
      val snapshot = (version, timestamp) match {
        case (Some(v), _) => deltaLog.getSnapshotAt(v)
        case (_, Some(ts)) =>
          val tsValue = java.sql.Timestamp.valueOf(ts)
          deltaLog.getSnapshotAt(
            deltaLog.history.getActiveCommitAtTime(
              tsValue, None, canReturnLastCommit = true).version)
        case _ => deltaLog.update()
      }

      val addFiles = predicate match {
        case Some(predicateStr) =>
          val pred = spark.sessionState.sqlParser.parseExpression(predicateStr)
          val resolvedPred = resolvePredicateAgainstSnapshot(spark, pred, snapshot)
          snapshot.filesForScan(Seq(resolvedPred)).files
        case None =>
          snapshot.allFiles.collect().toSeq
      }

      var df = spark.read.format("delta")
      version.foreach(v => df = df.option("versionAsOf", v))
      timestamp.foreach(ts => df = df.option("timestampAsOf", ts))
      var resultDf = df.load(tablePath.toString)
      predicate.foreach(p => resultDf = resultDf.filter(p))
      columns.foreach(cols => resultDf = resultDf.select(cols.map(columnRef): _*))

      val totalFileCount = snapshot.allFiles.count()
      resultDf = resultDf.cache()
      val actualCount = resultDf.count()

      ReadSuccess(resultDf, actualCount, addFiles, totalFileCount)
    } catch {
      case e: Exception =>
        val errorCode = e match {
          case st: org.apache.spark.SparkThrowable =>
            Option(st.getErrorClass).getOrElse(e.getClass.getSimpleName)
          case _ => e.getClass.getSimpleName
        }
        ReadError(errorCode, Option(e.getMessage).getOrElse(""))
    }

    // Everything below is infrastructure — failures here propagate (not captured as error specs)
    attempt match {
      case ReadSuccess(resultDf, actualCount, addFiles, totalFileCount) =>
        try {
          writeSuccessSpec(specsDir, specName, version, timestamp, predicate, columns,
            actualCount, addFiles.length, totalFileCount)
          writeExpectedData(spark, expectedDir, resultDf, actualCount, specName)
          writeExpectedMetadata(spark, expectedDir, addFiles)
          writeSummary(expectedDir, actualCount, addFiles.length, totalFileCount)

          // Post-capture validation: re-read and compare
          validateCapturedRead(spark, tablePath, specsDir, expectedDir, specName,
            version, timestamp, predicate, columns)

          println(s"  Read spec captured: $specName ($actualCount rows, " +
            s"${addFiles.length}/$totalFileCount files after data skipping)")
        } finally {
          resultDf.unpersist()
        }

      case ReadError(errorCode, errorMessage) =>
        // Re-validate: attempt the read against the COPIED table to confirm
        // the error is reproducible (not transient)
        validateErrorReproducible(spark, tablePath, specName, version, timestamp,
          predicate, columns, errorCode)

        writeErrorSpec(specsDir, specName, version, timestamp, predicate, columns,
          errorCode, errorMessage)
        println(s"  Read spec captured (error): $specName [$errorCode] $errorMessage")
    }
  }

  // --- Spec writing ---

  private def writeSuccessSpec(
      specsDir: Path, specName: String,
      version: Option[Long], timestamp: Option[String],
      predicate: Option[String], columns: Option[Seq[String]],
      rowCount: Long, fileCount: Int, totalFileCount: Long): Unit = {
    val spec = new java.util.LinkedHashMap[String, Any]()
    spec.put("type", "read")
    version.foreach(v => spec.put("version", v.asInstanceOf[AnyRef]))
    timestamp.foreach(ts => spec.put("timestamp", ts))
    predicate.foreach(p => spec.put("predicate", p))
    columns.foreach(cols => spec.put("columns", cols.asJava))
    val expected = new java.util.LinkedHashMap[String, Any]()
    expected.put("rowCount", rowCount.asInstanceOf[AnyRef])
    expected.put("fileCount", fileCount.asInstanceOf[AnyRef])
    expected.put("filesSkipped", (totalFileCount - fileCount).asInstanceOf[AnyRef])
    spec.put("expected", expected)
    JsonUtil.writeJson(specsDir.resolve(s"$specName.json"), spec)
  }

  private def writeErrorSpec(
      specsDir: Path, specName: String,
      version: Option[Long], timestamp: Option[String],
      predicate: Option[String], columns: Option[Seq[String]],
      errorCode: String, errorMessage: String): Unit = {
    val spec = new java.util.LinkedHashMap[String, Any]()
    spec.put("type", "read")
    version.foreach(v => spec.put("version", v.asInstanceOf[AnyRef]))
    timestamp.foreach(ts => spec.put("timestamp", ts))
    predicate.foreach(p => spec.put("predicate", p))
    columns.foreach(cols => spec.put("columns", cols.asJava))
    val error = new java.util.LinkedHashMap[String, Any]()
    error.put("errorCode", errorCode)
    error.put("errorMessage", errorMessage)
    spec.put("error", error)
    JsonUtil.writeJson(specsDir.resolve(s"$specName.json"), spec)
  }

  // --- Expected data writing ---

  private def writeExpectedData(
      spark: SparkSession, expectedDir: Path, resultDf: DataFrame,
      actualCount: Long, specName: String): Unit = {
    val expectedDataPath = expectedDir.resolve("expected_data")
    if (expectedDataPath.toFile.exists()) FileUtils.deleteDirectory(expectedDataPath.toFile)
    if (actualCount <= MaxExpectedDataRows) {
      resultDf.write.mode(SaveMode.Overwrite).parquet(expectedDataPath.toString)
    } else {
      System.err.println(s"WARN: Skipping expected_data for $specName: " +
        s"rowCount=$actualCount exceeds threshold=$MaxExpectedDataRows")
    }
  }

  private def writeExpectedMetadata(
      spark: SparkSession, expectedDir: Path,
      addFiles: Seq[org.apache.spark.sql.delta.actions.AddFile]): Unit = {
    val expectedMetaPath = expectedDir.resolve("expected_metadata")
    if (expectedMetaPath.toFile.exists()) FileUtils.deleteDirectory(expectedMetaPath.toFile)
    if (addFiles.nonEmpty) {
      val addFilesJson = addFiles.map(_.json)
      val addFilesDF = spark.createDataset(addFilesJson)(
        org.apache.spark.sql.Encoders.STRING).toDF("action")
      addFilesDF.write.mode(SaveMode.Overwrite).parquet(expectedMetaPath.toString)
    }
  }

  private def writeSummary(
      expectedDir: Path, actualCount: Long, fileCount: Int, totalFileCount: Long): Unit = {
    val summary = new java.util.LinkedHashMap[String, Any]()
    summary.put("actual_row_count", actualCount.asInstanceOf[AnyRef])
    summary.put("file_count", fileCount.asInstanceOf[AnyRef])
    summary.put("total_file_count", totalFileCount.asInstanceOf[AnyRef])
    summary.put("files_skipped", (totalFileCount - fileCount).asInstanceOf[AnyRef])
    JsonUtil.writeJson(expectedDir.resolve("summary.json"), summary)
  }

  // --- Post-capture validation ---

  /**
   * Confirm an error is reproducible by re-attempting the read against the copied table.
   * If the re-attempt succeeds, the original error was transient — fail loudly.
   */
  private def validateErrorReproducible(
      spark: SparkSession,
      tablePath: Path,
      specName: String,
      version: Option[Long],
      timestamp: Option[String],
      predicate: Option[String],
      columns: Option[Seq[String]],
      originalErrorCode: String): Unit = {
    DeltaLog.clearCache()
    val reAttemptSucceeded = try {
      var df = spark.read.format("delta")
      version.foreach(v => df = df.option("versionAsOf", v))
      timestamp.foreach(ts => df = df.option("timestampAsOf", ts))
      var resultDf = df.load(tablePath.toString)
      predicate.foreach(p => resultDf = resultDf.filter(p))
      columns.foreach(cols => resultDf = resultDf.select(cols.map(columnRef): _*))
      resultDf.count() // force materialization
      true
    } catch {
      case _: Exception => false // Re-attempt also failed — error is reproducible
    }
    if (reAttemptSucceeded) {
      throw new RuntimeException(
        s"Error spec validation FAILED for $specName: original read failed with " +
          s"[$originalErrorCode] but re-read against copied table succeeded. " +
          "The error was transient — not a valid error spec.")
    }
  }

  /**
   * Re-execute the read against the COPIED table and verify results match
   * the expected data row-for-row. Uses canonical JSON multiset comparison
   * for order-independent, type-safe validation (handles binary, nested, etc.).
   */
  private def validateCapturedRead(
      spark: SparkSession,
      tablePath: Path,
      specsDir: Path,
      expectedDir: Path,
      specName: String,
      version: Option[Long],
      timestamp: Option[String],
      predicate: Option[String],
      columns: Option[Seq[String]]): Unit = {

    // Re-read from copied table (clear cache for true independence)
    DeltaLog.clearCache()
    var df = spark.read.format("delta")
    version.foreach(v => df = df.option("versionAsOf", v))
    timestamp.foreach(ts => df = df.option("timestampAsOf", ts))
    var rereadDf = df.load(tablePath.toString)
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
        val countMismatch = (expectedMultiset.keySet & rereadMultiset.keySet)
          .filter(k => expectedMultiset(k) != rereadMultiset(k))

        val details = new StringBuilder()
        if (missing.nonEmpty) {
          details.append(s"\n  Missing rows (in expected but not re-read): ${missing.size}")
          missing.take(3).foreach(r => details.append(s"\n    $r"))
          if (missing.size > 3) details.append(s"\n    ... and ${missing.size - 3} more")
        }
        if (extra.nonEmpty) {
          details.append(s"\n  Extra rows (in re-read but not expected): ${extra.size}")
          extra.take(3).foreach(r => details.append(s"\n    $r"))
          if (extra.size > 3) details.append(s"\n    ... and ${extra.size - 3} more")
        }
        if (countMismatch.nonEmpty) {
          details.append(s"\n  Count mismatches: ${countMismatch.size}")
          countMismatch.take(3).foreach { k =>
            details.append(s"\n    $k: expected=${expectedMultiset(k)} got=${rereadMultiset(k)}")
          }
        }
        throw new RuntimeException(
          s"Post-capture validation FAILED for $specName: " +
            s"row-level data mismatch (expected ${expectedMultiset.values.sum} rows, " +
            s"got ${rereadMultiset.values.sum} rows)$details")
      }
    }

    // Also verify spec JSON row count matches
    val specFile = specsDir.resolve(s"$specName.json")
    if (Files.exists(specFile)) {
      val specNode = mapper.readTree(Files.readAllBytes(specFile))
      if (specNode.has("expected")) {
        val specRowCount = specNode.get("expected").get("rowCount").asLong()
        val rereadCount = rereadDf.count()
        require(rereadCount == specRowCount,
          s"Post-capture validation FAILED for $specName: " +
            s"re-read count=$rereadCount != spec rowCount=$specRowCount")
      }
    }
  }

  /**
   * Convert a DataFrame to a multiset of canonical JSON rows.
   * Order-independent, handles all types (binary, nested structs, maps, arrays).
   */
  private def toRowMultiset(df: DataFrame): Map[String, Int] = {
    df.toJSON.collect().toSeq.groupBy(identity).map {
      case (value, rows) => value -> rows.size
    }
  }

  private def resolvePredicateAgainstSnapshot(
      spark: SparkSession,
      predicate: Expression,
      snapshot: org.apache.spark.sql.delta.Snapshot): Expression = {
    val schema = snapshot.metadata.schema
    val resolver = spark.sessionState.conf.resolver
    def resolveExpr(expr: Expression): Expression = expr.transform {
      case u: UnresolvedAttribute =>
        val fieldName = u.nameParts.mkString(".")
        schema.find(f => resolver(f.name, fieldName)) match {
          case Some(field) =>
            AttributeReference(field.name, field.dataType, field.nullable)()
          case None =>
            snapshot.metadata.partitionColumns
              .find(c => resolver(c, fieldName))
              .flatMap(partCol => schema.find(f => resolver(f.name, partCol)))
              .map(field => AttributeReference(field.name, field.dataType, field.nullable)())
              .getOrElse(u)
        }
    }
    resolveExpr(predicate)
  }

  private def columnRef(name: String): Column = {
    if (name.startsWith("_metadata.")) col(name)
    else { val escaped = name.replace("`", "``"); col(s"`$escaped`") }
  }
}
