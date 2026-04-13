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

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.delta.DeltaLog

object ReadCapture {

  private val MaxExpectedDataRows = 5_000_000L

  def capture(
      spark: SparkSession, testId: String, tablePath: Path,
      outputDir: Path, specsDir: Path, name: String,
      predicate: Option[String] = None, version: Option[Long] = None,
      timestamp: Option[String] = None, columns: Option[Seq[String]] = None): Unit = {

    val specName = s"${testId}_$name"
    require(!(version.isDefined && timestamp.isDefined),
      s"Read $specName: cannot specify both version and timestamp")

    val expectedDir = outputDir.resolve("expected").resolve(specName)
    Files.createDirectories(expectedDir)
    val specPath = specsDir.resolve(s"$specName.json")

    val (expected, expectedError, addFilesJson) = try {
      val deltaLog = DeltaLog.forTable(spark, tablePath.toString)
      val snapshot = JsonUtil.resolveSnapshot(deltaLog, version, timestamp)

      var df = JsonUtil.buildDeltaReader(spark, tablePath, version, timestamp)
      df = JsonUtil.applyFilters(df, predicate, columns)
      df = df.cache()
      val count = df.count()

      // Get input files from the executed query to determine data skipping
      val inputFilePaths = df.inputFiles.map(_.split("/").last).toSet
      val allAddFiles = snapshot.allFiles.collect()
      val scannedFiles = allAddFiles.filter(f => inputFilePaths.contains(f.path.split("/").last))
      val addFilesJson = scannedFiles.map(_.json).toSeq
      val totalFileCount = allAddFiles.length.toLong

      try {
        writeExpectedData(expectedDir, df, count, specName)
        writeExpectedMetadata(spark, expectedDir, addFilesJson)
        (Some(ReadExpected(count, addFilesJson.length, totalFileCount - addFilesJson.length)), None, addFilesJson)
      } finally {
        df.unpersist()
      }
    } catch {
      case e: Exception =>
        (None, Some(SpecError(JsonUtil.extractErrorCode(e), Option(e.getMessage).getOrElse(""))), Seq.empty[String])
    }

    val spec = ReadSpec(version, timestamp, predicate, columns, expected, expectedError)
    JsonUtil.writeSpec(specPath, spec)
    validateFromSpec(spark, tablePath, expectedDir, specPath, addFilesJson)

    (expected, expectedError) match {
      case (Some(exp), _) =>
        println(s"  Read captured: $specName (${exp.rowCount} rows, ${exp.fileCount}/${exp.fileCount + exp.filesSkipped} files)")
      case (_, Some(err)) =>
        println(s"  Read captured (error): $specName [${err.errorCode}] ${err.errorMessage}")
      case _ =>
    }
  }

  private def writeExpectedData(expectedDir: Path, df: DataFrame, count: Long, specName: String): Unit = {
    val path = expectedDir.resolve("expected_data")
    if (path.toFile.exists()) FileUtils.deleteDirectory(path.toFile)
    if (count <= MaxExpectedDataRows) {
      df.write.mode(SaveMode.Overwrite).parquet(path.toString)
    } else {
      System.err.println(s"WARN: Skipping expected_data for $specName: $count rows exceeds limit")
    }
  }

  private def writeExpectedMetadata(spark: SparkSession, expectedDir: Path, addFilesJson: Seq[String]): Unit = {
    val path = expectedDir.resolve("expected_metadata")
    if (path.toFile.exists()) FileUtils.deleteDirectory(path.toFile)
    if (addFilesJson.nonEmpty) {
      spark.createDataset(addFilesJson)(org.apache.spark.sql.Encoders.STRING)
        .toDF("action").write.mode(SaveMode.Overwrite).parquet(path.toString)
    }
  }

  private[workload] def validateFromSpec(spark: SparkSession, tablePath: Path,
      expectedDir: Path, specPath: Path, originalAddFilesJson: Seq[String]): Unit = {
    val spec = JsonUtil.readReadSpec(specPath)
    val specName = specPath.getFileName.toString.stripSuffix(".json")

    (spec.expected, spec.expectedError) match {
      case (Some(_), _) =>
        val rereadDf = JsonUtil.applyFilters(
          JsonUtil.buildDeltaReader(spark, tablePath, spec.version, spec.timestamp),
          spec.predicate, spec.columns)

        val expectedDataPath = expectedDir.resolve("expected_data")
        if (Files.exists(expectedDataPath)) {
          JsonUtil.assertMultisetsEqual(
            JsonUtil.toRowMultiset(spark.read.parquet(expectedDataPath.toString)),
            JsonUtil.toRowMultiset(rereadDf),
            specName)
        }

        val expectedMetaPath = expectedDir.resolve("expected_metadata")
        if (Files.exists(expectedMetaPath)) {
          val written = spark.read.parquet(expectedMetaPath.toString).collect().map(_.getString(0)).sorted
          require(written.sameElements(originalAddFilesJson.sorted),
            s"Metadata validation FAILED for $specName")
        }

      case (_, Some(err)) =>
        val actualCode = try {
          JsonUtil.applyFilters(
            JsonUtil.buildDeltaReader(spark, tablePath, spec.version, spec.timestamp),
            spec.predicate, spec.columns).count()
          None
        } catch {
          case e: Exception => Some(JsonUtil.extractErrorCode(e))
        }
        require(actualCode.isDefined,
          s"Error validation FAILED for $specName: expected operation to fail but it succeeded")
        if (actualCode.get != err.errorCode) {
          System.err.println(s"WARN: Error code mismatch for $specName: " +
            s"captured '${err.errorCode}' but got '${actualCode.get}'")
        }

      case _ =>
    }
  }
}
