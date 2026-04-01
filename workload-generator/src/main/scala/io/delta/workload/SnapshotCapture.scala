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

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
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

  private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)

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

    val deltaLog = DeltaLog.forTable(spark, tablePath.toString)
    DeltaLog.clearCache()
    val snapshot = (version, timestamp) match {
      case (Some(v), _) =>
        try {
          deltaLog.getSnapshotAt(v)
        } catch {
          case NonFatal(_) =>
            val latest = deltaLog.update()
            require(latest.version == v,
              s"Snapshot version mismatch: expected=$v, got=${latest.version}")
            latest
        }
      case (_, Some(ts)) =>
        val tsValue = java.sql.Timestamp.valueOf(ts)
        deltaLog.getSnapshotAt(
          deltaLog.history.getActiveCommitAtTime(
            tsValue, canReturnLastCommit = true).version)
      case _ => deltaLog.update()
    }

    val specName = (version, timestamp) match {
      case (Some(v), _) => s"${testId}_snapshot_v$v"
      case (_, Some(ts)) => s"${testId}_snapshot_ts_${ts.replace(":", "-").replace(" ", "_")}"
      case _ => s"${testId}_snapshot"
    }

    // Build spec with inline expected protocol + metadata
    val snapshotSpec = new java.util.LinkedHashMap[String, Any]()
    snapshotSpec.put("type", "snapshotConstruction")
    (version.orElse(if (timestamp.isEmpty) Some(snapshot.version) else None))
      .foreach(v => snapshotSpec.put("version", v.asInstanceOf[AnyRef]))
    timestamp.foreach(ts => snapshotSpec.put("timestamp", ts))

    val expectedBlock = new java.util.LinkedHashMap[String, Any]()

    // Protocol: Action.json produces {"protocol":{...}} - extract inner
    val protocolTree = mapper.readTree(snapshot.protocol.json)
    expectedBlock.put("protocol", mapper.treeToValue(
      protocolTree.get("protocol"), classOf[Any]))

    // Metadata: Action.json produces {"metaData":{...}} - extract inner
    val metadataTree = mapper.readTree(snapshot.metadata.json)
    expectedBlock.put("metadata", mapper.treeToValue(
      metadataTree.get("metaData"), classOf[Any]))

    snapshotSpec.put("expected", expectedBlock)

    val specFile = specsDir.resolve(s"$specName.json")
    JsonUtil.writeJson(specFile, snapshotSpec)

    // Post-capture validation: re-read and compare
    validate(spark, specName, tablePath, specsDir)

    println(s"  Snapshot spec captured: $specName (version=${snapshot.version})")
  }

  /** Validate a captured snapshot spec by re-loading and comparing. */
  private def validate(
      spark: SparkSession,
      specName: String,
      tablePath: Path,
      specsDir: Path): Unit = {
    val specFile = specsDir.resolve(s"$specName.json")
    require(Files.exists(specFile), s"Snapshot spec file missing after capture: $specFile")
    val spec = mapper.readTree(Files.readAllBytes(specFile))

    val deltaLog = DeltaLog.forTable(spark, tablePath.toString)
    val snapshot = if (spec.has("version")) {
      val targetVersion = spec.get("version").asLong()
      try { deltaLog.getSnapshotAt(targetVersion) }
      catch {
        case NonFatal(_) =>
          val latest = deltaLog.update()
          require(latest.version == targetVersion,
            s"Snapshot version mismatch for $specName")
          latest
      }
    } else if (spec.has("timestamp")) {
      val ts = java.sql.Timestamp.valueOf(spec.get("timestamp").asText())
      deltaLog.getSnapshotAt(
        deltaLog.history.getActiveCommitAtTime(ts, canReturnLastCommit = true).version)
    } else {
      deltaLog.update()
    }

    require(spec.has("expected"), s"Snapshot spec $specName missing 'expected' block")
    val expected = spec.get("expected")

    if (expected.has("protocol")) {
      val expectedProto = expected.get("protocol")
      val actualProtoTree = mapper.readTree(snapshot.protocol.json).get("protocol")
      if (expectedProto.has("minReaderVersion")) {
        val expectedRV = expectedProto.get("minReaderVersion").asInt()
        val actualRV = actualProtoTree.get("minReaderVersion").asInt()
        require(expectedRV == actualRV,
          s"Snapshot validation failed for $specName: " +
            s"protocol.minReaderVersion expected=$expectedRV actual=$actualRV")
      }
      if (expectedProto.has("minWriterVersion")) {
        val expectedWV = expectedProto.get("minWriterVersion").asInt()
        val actualWV = actualProtoTree.get("minWriterVersion").asInt()
        require(expectedWV == actualWV,
          s"Snapshot validation failed for $specName: " +
            s"protocol.minWriterVersion expected=$expectedWV actual=$actualWV")
      }
    }

    if (expected.has("metadata")) {
      val expectedMeta = expected.get("metadata")
      val actualMetaTree = mapper.readTree(snapshot.metadata.json).get("metaData")
      val expectedNorm = mapper.readTree(mapper.writeValueAsString(expectedMeta))
      val actualNorm = mapper.readTree(
        mapper.writeValueAsString(mapper.treeToValue(actualMetaTree, classOf[Any])))
      require(expectedNorm.equals(actualNorm),
        s"Snapshot validation failed for $specName: metadata mismatch")
    }
  }
}
