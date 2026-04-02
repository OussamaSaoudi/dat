/*
 * Copyright (2024) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use the file except in compliance with the License.
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

import scala.util.control.NonFatal

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog

/**
 * Captures snapshot construction specs and expected protocol/metadata.
 *
 * For each snapshot spec, generates:
 * - specs/<testId>_snapshot[_v<N>|_ts_<ts>].json
 *
 * The spec JSON includes inline expected protocol and metadata, extracted from
 * Delta's Action.json serialization format.
 */
object SnapshotCapture {

  

  /**
   * Capture a snapshot construction spec.
   *
   * @param spark     SparkSession
   * @param testId    Test identifier (used in spec filenames)
   * @param tablePath Path to the Delta table (the copied version)
   * @param specsDir  Directory to write spec JSON files
   * @param version   Optional: snapshot at specific version
   * @param timestamp Optional: snapshot at specific timestamp
   */
  def capture(
      spark: SparkSession,
      testId: String,
      tablePath: Path,
      specsDir: Path,
      version: Option[Long] = None,
      timestamp: Option[String] = None): Unit = {
    require(!(version.isDefined && timestamp.isDefined),
      "Cannot specify both version and timestamp for snapshot construction")

    val specName = (version, timestamp) match {
      case (Some(v), _) => s"${testId}_snapshot_v$v"
      case (_, Some(ts)) => s"${testId}_snapshot_ts_${ts.replace(":", "-").replace(" ", "_")}"
      case _ => s"${testId}_snapshot"
    }

    val snapshotAttempt: Either[(String, String), org.apache.spark.sql.delta.Snapshot] = try {
      DeltaLog.clearCache()
      val deltaLog = DeltaLog.forTable(spark, tablePath.toString)
      val snapshot = (version, timestamp) match {
        case (Some(v), _) => deltaLog.getSnapshotAt(v)
        case (_, Some(ts)) =>
          val tsValue = java.sql.Timestamp.valueOf(ts)
          deltaLog.getSnapshotAt(
            deltaLog.history.getActiveCommitAtTime(
              tsValue, canReturnLastCommit = true, mustBeRecreatable = false, canReturnEarliestCommit = false).version)
        case _ => deltaLog.update()
      }
      Right(snapshot)
    } catch {
      case e: Exception =>
        Left((JsonUtil.extractErrorCode(e), Option(e.getMessage).getOrElse("")))
    }

    snapshotAttempt match {
      case Right(snapshot) =>
        // Build spec with inline expected protocol + metadata
        val snapshotSpec = new java.util.LinkedHashMap[String, Any]()
        snapshotSpec.put("type", "snapshot")
        (version.orElse(if (timestamp.isEmpty) Some(snapshot.version) else None))
          .foreach(v => snapshotSpec.put("version", v.asInstanceOf[AnyRef]))
        timestamp.foreach(ts => snapshotSpec.put("timestamp", ts))

        val expectedBlock = new java.util.LinkedHashMap[String, Any]()

        val protocolTree = JsonUtil.mapper.readTree(snapshot.protocol.json)
        expectedBlock.put("protocol", JsonUtil.mapper.treeToValue(
          protocolTree.get("protocol"), classOf[Any]))

        val metadataTree = JsonUtil.mapper.readTree(snapshot.metadata.json)
        expectedBlock.put("metadata", JsonUtil.mapper.treeToValue(
          metadataTree.get("metaData"), classOf[Any]))

        snapshotSpec.put("expected", expectedBlock)

        val specFile = specsDir.resolve(s"$specName.json")
        JsonUtil.writeJson(specFile, snapshotSpec)

        // Post-capture validation: re-read and compare
        validate(spark, specName, tablePath, specsDir)

        println(s"  Snapshot spec captured: $specName (version=${snapshot.version})")

      case Left((errorCode, errorMessage)) =>
        // Validate error reproducibility: retry and confirm same error
        val retryErrorCode = try {
          DeltaLog.clearCache()
          val dl = DeltaLog.forTable(spark, tablePath.toString)
          (version, timestamp) match {
            case (Some(v), _) => dl.getSnapshotAt(v)
            case (_, Some(ts)) =>
              val tsValue = java.sql.Timestamp.valueOf(ts)
              dl.getSnapshotAt(
                dl.history.getActiveCommitAtTime(
                  tsValue, canReturnLastCommit = true,
                  mustBeRecreatable = false,
                  canReturnEarliestCommit = false).version)
            case _ => dl.update()
          }
          None // Succeeded on retry — original error was transient
        } catch {
          case e: Exception => Some(JsonUtil.extractErrorCode(e))
        }
        require(retryErrorCode.isDefined,
          s"Snapshot error validation FAILED for $specName: " +
            s"original error [$errorCode] did not reproduce on retry")
        require(retryErrorCode.get == errorCode,
          s"Snapshot error validation FAILED for $specName: " +
            s"original errorCode=[$errorCode] but retry errorCode=[${retryErrorCode.get}]")

        // Write error spec with unique name (includes version/timestamp suffix)
        val snapshotSpec = new java.util.LinkedHashMap[String, Any]()
        snapshotSpec.put("type", "snapshot")
        version.foreach(v => snapshotSpec.put("version", v.asInstanceOf[AnyRef]))
        timestamp.foreach(ts => snapshotSpec.put("timestamp", ts))
        snapshotSpec.put("name", testId)

        val error = new java.util.LinkedHashMap[String, Any]()
        error.put("errorCode", errorCode)
        error.put("errorMessage", errorMessage)
        snapshotSpec.put("error", error)

        val specFile = specsDir.resolve(s"$specName.json")
        JsonUtil.writeJson(specFile, snapshotSpec)

        println(s"  Snapshot spec captured (error): $specName [$errorCode] $errorMessage")
    }
  }

  /** Validate a captured snapshot spec by re-loading and comparing. */
  private def validate(
      spark: SparkSession,
      specName: String,
      tablePath: Path,
      specsDir: Path): Unit = {
    val specFile = specsDir.resolve(s"$specName.json")
    require(Files.exists(specFile), s"Snapshot spec file missing after capture: $specFile")
    val spec = JsonUtil.mapper.readTree(Files.readAllBytes(specFile))

    DeltaLog.clearCache()
    val deltaLog = DeltaLog.forTable(spark, tablePath.toString)
    val snapshot = if (spec.has("version")) {
      deltaLog.getSnapshotAt(spec.get("version").asLong())
    } else if (spec.has("timestamp")) {
      val ts = java.sql.Timestamp.valueOf(spec.get("timestamp").asText())
      deltaLog.getSnapshotAt(
        deltaLog.history.getActiveCommitAtTime(ts, canReturnLastCommit = true, mustBeRecreatable = false, canReturnEarliestCommit = false).version)
    } else {
      deltaLog.update()
    }

    require(spec.has("expected"), s"Snapshot spec $specName missing 'expected' block")
    val expected = spec.get("expected")

    // Validate resolved version
    if (expected.has("version")) {
      val expectedVersion = expected.get("version").asLong()
      require(snapshot.version == expectedVersion,
        s"Snapshot validation failed for $specName: " +
          s"version expected=$expectedVersion actual=${snapshot.version}")
    }

    if (expected.has("protocol")) {
      val expectedProto = JsonUtil.mapper.readTree(JsonUtil.mapper.writeValueAsString(expected.get("protocol")))
      val actualProto = JsonUtil.mapper.readTree(
        JsonUtil.mapper.writeValueAsString(
          JsonUtil.mapper.treeToValue(JsonUtil.mapper.readTree(snapshot.protocol.json).get("protocol"), classOf[Any])))
      require(expectedProto.equals(actualProto),
        s"Snapshot validation failed for $specName: protocol mismatch\n" +
          s"  expected: $expectedProto\n  actual: $actualProto")
    }

    if (expected.has("metadata")) {
      val expectedMeta = JsonUtil.mapper.readTree(JsonUtil.mapper.writeValueAsString(expected.get("metadata")))
      val actualMeta = JsonUtil.mapper.readTree(
        JsonUtil.mapper.writeValueAsString(
          JsonUtil.mapper.treeToValue(JsonUtil.mapper.readTree(snapshot.metadata.json).get("metaData"), classOf[Any])))
      require(expectedMeta.equals(actualMeta),
        s"Snapshot validation failed for $specName: metadata mismatch")
    }
  }
}
