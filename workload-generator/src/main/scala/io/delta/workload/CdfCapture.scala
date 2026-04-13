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

object CdfCapture {

  def capture(
      spark: SparkSession, testId: String, tablePath: Path,
      outputDir: Path, specsDir: Path, name: String,
      startVersion: Option[Long] = None, endVersion: Option[Long] = None,
      startTimestamp: Option[String] = None, endTimestamp: Option[String] = None,
      predicate: Option[String] = None, columns: Option[Seq[String]] = None): Unit = {

    val specName = s"${testId}_$name"
    require(startVersion.isDefined || startTimestamp.isDefined,
      s"CDF $specName: start bound required")
    require(!(startVersion.isDefined && startTimestamp.isDefined),
      s"CDF $specName: cannot specify both startVersion and startTimestamp")
    require(!(endVersion.isDefined && endTimestamp.isDefined),
      s"CDF $specName: cannot specify both endVersion and endTimestamp")

    val expectedDir = outputDir.resolve("expected").resolve(specName)
    Files.createDirectories(expectedDir)
    val specPath = specsDir.resolve(s"$specName.json")

    val (expected, expectedError) = try {
      val latestVersion = DeltaLog.forTable(spark, tablePath.toString).update().version
      var df = buildCdfReader(spark, tablePath, startVersion, endVersion,
        startTimestamp, endTimestamp, latestVersion)
      df = JsonUtil.applyFilters(df, predicate, columns)
      df = df.cache()
      val count = df.count()

      try {
        val expectedDataPath = expectedDir.resolve("expected_data")
        if (expectedDataPath.toFile.exists()) FileUtils.deleteDirectory(expectedDataPath.toFile)
        df.write.mode(SaveMode.Overwrite).parquet(expectedDataPath.toString)
        (Some(CdfExpected(count)), None)
      } finally {
        df.unpersist()
      }
    } catch {
      case e: Exception =>
        (None, Some(SpecError(JsonUtil.extractErrorCode(e), Option(e.getMessage).getOrElse(""))))
    }

    val spec = CdfSpec(startVersion, endVersion, startTimestamp, endTimestamp, predicate, columns, expected, expectedError)
    JsonUtil.writeSpec(specPath, spec)
    validateFromSpec(spark, tablePath, expectedDir, specPath)

    (expected, expectedError) match {
      case (Some(exp), _) => println(s"  CDF captured: $specName (${exp.rowCount} rows)")
      case (_, Some(err)) => println(s"  CDF captured (error): $specName [${err.errorCode}] ${err.errorMessage}")
      case _ =>
    }
  }

  private def buildCdfReader(spark: SparkSession, tablePath: Path,
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

  private def buildReaderFromSpec(spark: SparkSession, tablePath: Path, spec: CdfSpec): DataFrame = {
    val latestVersion = DeltaLog.forTable(spark, tablePath.toString).update().version
    var df = buildCdfReader(spark, tablePath, spec.startVersion, spec.endVersion,
      spec.startTimestamp, spec.endTimestamp, latestVersion)
    JsonUtil.applyFilters(df, spec.predicate, spec.columns)
  }

  private[workload] def validateFromSpec(
      spark: SparkSession, tablePath: Path, expectedDir: Path, specPath: Path): Unit = {
    val spec = JsonUtil.readCdfSpec(specPath)
    val specName = specPath.getFileName.toString.stripSuffix(".json")

    (spec.expected, spec.expectedError) match {
      case (Some(exp), _) =>
        val rereadDf = buildReaderFromSpec(spark, tablePath, spec)
        val expectedDataPath = expectedDir.resolve("expected_data")
        if (Files.exists(expectedDataPath)) {
          val expectedDf = spark.read.parquet(expectedDataPath.toString)
          JsonUtil.assertMultisetsEqual(
            JsonUtil.toRowMultiset(expectedDf),
            JsonUtil.toRowMultiset(rereadDf),
            specName)
        } else {
          require(rereadDf.count() == exp.rowCount,
            s"Validation FAILED for $specName: count mismatch")
        }

      case (_, Some(err)) =>
        val actualCode = try {
          buildReaderFromSpec(spark, tablePath, spec).count()
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
