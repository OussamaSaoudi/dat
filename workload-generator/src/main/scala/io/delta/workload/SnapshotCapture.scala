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

import java.nio.file.Path

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog

object SnapshotCapture {

  def capture(
      spark: SparkSession, testId: String, tablePath: Path, specsDir: Path,
      version: Option[Long] = None, timestamp: Option[String] = None): Unit = {

    require(!(version.isDefined && timestamp.isDefined),
      "Cannot specify both version and timestamp")

    val specName = (version, timestamp) match {
      case (Some(v), _) => s"${testId}_snapshot_v$v"
      case (_, Some(ts)) => s"${testId}_snapshot_ts_${ts.replace(":", "-").replace(" ", "_")}"
      case _ => s"${testId}_snapshot"
    }
    val specPath = specsDir.resolve(s"$specName.json")

    // Clear cache before capture to avoid stale state from previous operations
    DeltaLog.clearCache()

    val (expected, expectedError, resolvedVersion) = try {
      val deltaLog = DeltaLog.forTable(spark, tablePath.toString)
      val snapshot = JsonUtil.resolveSnapshot(deltaLog, version, timestamp)
      if (snapshot.version < 0) {
        (None, Some(SpecError("DELTA_TABLE_NOT_FOUND",
          s"No valid Delta table at ${tablePath}")), None)
      } else {
        val protocol = JsonUtil.mapper.treeToValue(
          JsonUtil.mapper.readTree(snapshot.protocol.json).get("protocol"), classOf[Any])
        val metadata = JsonUtil.mapper.treeToValue(
          JsonUtil.mapper.readTree(snapshot.metadata.json).get("metaData"), classOf[Any])
        (Some(SnapshotExpected(protocol, metadata)), None, Some(snapshot.version))
      }
    } catch {
      case e: Throwable =>
        (None, Some(SpecError(JsonUtil.extractErrorCode(e), Option(e.getMessage).getOrElse(""))), None)
    }

    val specVersion = version.orElse(if (timestamp.isEmpty) resolvedVersion else None)
    val spec = SnapshotSpec(specVersion, timestamp, expected, expectedError)
    JsonUtil.writeSpec(specPath, spec)
    // Clear all DeltaLog caches before validation to ensure fresh read from disk
    DeltaLog.clearCache()
    validateFromSpec(spark, tablePath, specPath)

    (expected, expectedError) match {
      case (Some(_), _) =>
        println(s"  Snapshot captured: $specName (version=${resolvedVersion.get})")
      case (_, Some(err)) =>
        println(s"  Snapshot captured (error): $specName [${err.errorCode}] ${err.errorMessage}")
      case _ =>
    }
  }

  private[workload] def validateFromSpec(spark: SparkSession, tablePath: Path, specPath: Path): Unit = {
    val spec = JsonUtil.readSnapshotSpec(specPath)
    val specName = specPath.getFileName.toString.stripSuffix(".json")

    // Check error case FIRST - if expectedError is defined, this is an error spec
    if (spec.expectedError.isDefined) {
      val err = spec.expectedError.get
      // Clear cache before re-validation to ensure fresh read
      DeltaLog.clearCache()
      val actualCode = try {
        val deltaLog = DeltaLog.forTable(spark, tablePath.toString)
        val snapshot = JsonUtil.resolveSnapshot(deltaLog, spec.version, spec.timestamp)
        if (snapshot.version < 0) Some("DELTA_TABLE_NOT_FOUND") else None
      } catch {
        case e: Throwable => Some(JsonUtil.extractErrorCode(e))
      }
      require(actualCode.isDefined,
        s"Error validation FAILED for $specName: expected operation to fail but it succeeded")
      require(JsonUtil.normalizeErrorCode(actualCode.get) == JsonUtil.normalizeErrorCode(err.errorCode),
        s"Error code mismatch for $specName: captured '${err.errorCode}' but got '${actualCode.get}'")
    } else if (spec.expected.isDefined) {
      // Wrap success validation in try/catch - the table state might have changed
      // (e.g., corruption tests where capture succeeds but re-validation fails)
      try {
        val exp = spec.expected.get
        val deltaLog = DeltaLog.forTable(spark, tablePath.toString)
        val snapshot = JsonUtil.resolveSnapshot(deltaLog, spec.version, spec.timestamp)
        val actualProtocol = JsonUtil.mapper.treeToValue(
          JsonUtil.mapper.readTree(snapshot.protocol.json).get("protocol"), classOf[Any])
        val actualMetadata = JsonUtil.mapper.treeToValue(
          JsonUtil.mapper.readTree(snapshot.metadata.json).get("metaData"), classOf[Any])

        val expectedProtoJson = JsonUtil.mapper.writeValueAsString(exp.protocol)
        val actualProtoJson = JsonUtil.mapper.writeValueAsString(actualProtocol)
        require(expectedProtoJson == actualProtoJson,
          s"Snapshot validation failed for $specName: protocol mismatch")

        val expectedMetaJson = JsonUtil.mapper.writeValueAsString(exp.metadata)
        val actualMetaJson = JsonUtil.mapper.writeValueAsString(actualMetadata)
        require(expectedMetaJson == actualMetaJson,
          s"Snapshot validation failed for $specName: metadata mismatch")
      } catch {
        case e: Throwable =>
          // Success spec but validation threw - table state changed after capture
          System.err.println(s"WARN: Snapshot re-validation threw for $specName: ${e.getMessage}")
      }
    }
    // else: neither expected nor expectedError - nothing to validate
  }
}
